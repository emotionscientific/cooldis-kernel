use strict;
use warnings;

while (defined(my $file = shift @ARGV)) {
open my $fh, '<', $file or die "open $file: $!";
local $/;
my $source = <$fh>;
close $fh or die "close $file: $!";

my $code = $source;
my %markers;
my %empty_markers;
my $length = length $source;
my $offset = 0;
my $line = 1;

sub blank_span {
    my ($text) = @_;
    $text =~ s/[^\n]/ /g;
    return $text;
}

while ($offset < $length) {
    if (substr($source, $offset, 2) eq '//') {
        my $end = index($source, "\n", $offset);
        $end = $length if $end < 0;
        my $comment = substr($source, $offset, $end - $offset);
        if ($comment =~ m{//\s*tight-timeout:\s*(\S.*)\z}) {
            $markers{$line}++;
        } elsif ($comment =~ m{//\s*tight-timeout:}) {
            $empty_markers{$line} = 1;
        }
        substr($code, $offset, $end - $offset) = blank_span($comment);
        $offset = $end;
        next;
    }

    if (substr($source, $offset, 2) eq '/*') {
        my $start = $offset;
        my $depth = 1;
        $offset += 2;
        while ($offset < $length && $depth > 0) {
            if (substr($source, $offset, 2) eq '/*') {
                $depth++;
                $offset += 2;
            } elsif (substr($source, $offset, 2) eq '*/') {
                $depth--;
                $offset += 2;
            } else {
                $line++ if substr($source, $offset, 1) eq "\n";
                $offset++;
            }
        }
        my $comment = substr($source, $start, $offset - $start);
        substr($code, $start, $offset - $start) = blank_span($comment);
        next;
    }

    my $tail = substr($source, $offset, 260);
    if ($tail =~ /\A(?:b|c)?r(\#{0,255})"/) {
        my $hashes = $1;
        my $opening = length $&;
        my $closing = '"' . $hashes;
        my $end = index($source, $closing, $offset + $opening);
        $end = $length - length($closing) if $end < 0;
        $end += length $closing;
        my $literal = substr($source, $offset, $end - $offset);
        $line += ($literal =~ tr/\n//);
        substr($code, $offset, $end - $offset) = blank_span($literal);
        $offset = $end;
        next;
    }

    my $quote_offset = $offset;
    if (substr($source, $offset, 2) eq 'b"' || substr($source, $offset, 2) eq 'c"') {
        $quote_offset++;
    }
    if (substr($source, $quote_offset, 1) eq '"') {
        my $start = $offset;
        $offset = $quote_offset + 1;
        while ($offset < $length) {
            my $char = substr($source, $offset, 1);
            if ($char eq '\\') {
                $offset += 2;
                next;
            }
            $line++ if $char eq "\n";
            $offset++;
            last if $char eq '"';
        }
        my $literal = substr($source, $start, $offset - $start);
        substr($code, $start, $offset - $start) = blank_span($literal);
        next;
    }

    $line++ if substr($source, $offset, 1) eq "\n";
    $offset++;
}

for my $marker_line (sort { $a <=> $b } keys %empty_markers) {
    print "$file:$marker_line: tight-timeout marker requires a non-empty reason\n";
}

sub line_for_offset {
    my ($text, $position) = @_;
    my $prefix = substr($text, 0, $position);
    return 1 + ($prefix =~ tr/\n//);
}

sub literal_value {
    my ($raw) = @_;
    $raw =~ s/_//g;
    $raw =~ s/(?:u|i)(?:8|16|32|64|128|size)\z//;
    return 0 + $raw;
}

sub milliseconds {
    my ($unit, $raw) = @_;
    my $value = literal_value($raw);
    return $value * 1_000 if $unit eq 'secs';
    return $value if $unit eq 'millis';
    return $value / 1_000 if $unit eq 'micros';
    return $value / 1_000_000;
}

pos($code) = 0;
while ($code =~ /(?<![A-Za-z0-9_])timeout\s*\(/g) {
    my $call_offset = $-[0];
    my $call_line = line_for_offset($code, $call_offset);
    my $cursor = pos($code);
    my $argument_start = $cursor;
    my $depth = 0;
    while ($cursor < length $code) {
        my $char = substr($code, $cursor, 1);
        if ($char eq '(' || $char eq '[' || $char eq '{') {
            $depth++;
        } elsif ($char eq ')' || $char eq ']' || $char eq '}') {
            last if $char eq ')' && $depth == 0;
            $depth-- if $depth > 0;
        } elsif ($char eq ',' && $depth == 0) {
            last;
        }
        $cursor++;
    }
    my $argument = substr($code, $argument_start, $cursor - $argument_start);
    my $sub_floor = 0;

    while ($argument =~ /(?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*Duration\s*::\s*from_(secs|millis|micros|nanos)\s*\(\s*([0-9][0-9_]*(?:(?:u|i)(?:8|16|32|64|128|size))?)\s*\)/g) {
        $sub_floor = 1 if milliseconds($1, $2) < 30_000;
    }
    while ($argument =~ /(?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*Duration\s*::\s*new\s*\(\s*([0-9][0-9_]*(?:(?:u|i)(?:8|16|32|64|128|size))?)\s*,\s*([0-9][0-9_]*(?:(?:u|i)(?:8|16|32|64|128|size))?)\s*\)/g) {
        my $duration_ms = literal_value($1) * 1_000 + literal_value($2) / 1_000_000;
        $sub_floor = 1 if $duration_ms < 30_000;
    }
    next unless $sub_floor;

    my $marker_line = $markers{$call_line} ? $call_line
        : $markers{$call_line - 1} ? $call_line - 1
        : undef;
    if (defined $marker_line && $markers{$marker_line} > 0) {
        $markers{$marker_line}--;
        next;
    }
    print "$file:$call_line: timeout is below the 30-second floor without a tight-timeout reason\n";
}
}
