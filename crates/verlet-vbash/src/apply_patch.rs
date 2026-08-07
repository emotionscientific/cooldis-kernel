const BEGIN_PATCH_MARKER: &str = "*** Begin Patch";
const ENVIRONMENT_ID_MARKER: &str = "*** Environment ID: ";
const END_PATCH_MARKER: &str = "*** End Patch";
const ADD_FILE_MARKER: &str = "*** Add File: ";
const DELETE_FILE_MARKER: &str = "*** Delete File: ";
const UPDATE_FILE_MARKER: &str = "*** Update File: ";
const MOVE_TO_MARKER: &str = "*** Move to: ";
const EOF_MARKER: &str = "*** End of File";
const CHANGE_CONTEXT_MARKER: &str = "@@ ";
const EMPTY_CHANGE_CONTEXT_MARKER: &str = "@@";

#[derive(Clone, Debug, Eq, PartialEq)]
enum Hunk {
    AddFile {
        path: std::path::PathBuf,
        contents: String,
    },
    DeleteFile {
        path: std::path::PathBuf,
    },
    UpdateFile {
        path: std::path::PathBuf,
        move_path: Option<std::path::PathBuf>,
        chunks: Vec<UpdateFileChunk>,
    },
}

impl Hunk {
    fn path(&self) -> &std::path::Path {
        match self {
            Hunk::AddFile { path, .. } | Hunk::DeleteFile { path } => path,
            Hunk::UpdateFile {
                path,
                move_path: None,
                ..
            } => path,
            Hunk::UpdateFile {
                move_path: Some(path),
                ..
            } => path,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UpdateFileChunk {
    change_context: Option<String>,
    old_lines: Vec<String>,
    new_lines: Vec<String>,
    is_end_of_file: bool,
}

#[derive(Default)]
struct AffectedPaths {
    added: Vec<std::path::PathBuf>,
    modified: Vec<std::path::PathBuf>,
    deleted: Vec<std::path::PathBuf>,
}

pub async fn apply_patch_to_bashkit(
    fs: std::sync::Arc<dyn bashkit::FileSystem>,
    cwd: &std::path::Path,
    patch: &str,
) -> Result<String, String> {
    let hunks = parse_patch(patch)?;
    if hunks.is_empty() {
        return Err("No files were modified.".to_string());
    }

    let mut affected = AffectedPaths::default();
    for hunk in hunks {
        let affected_path = hunk.path().to_path_buf();
        match hunk {
            Hunk::AddFile { path, contents } => {
                let target = resolve_virtual_path(cwd, &path);
                write_file_with_missing_parent_retry(fs.as_ref(), &target, contents.into_bytes())
                    .await
                    .map_err(|err| format!("Failed to write file {}: {err}", target.display()))?;
                affected.added.push(affected_path);
            }
            Hunk::DeleteFile { path } => {
                let target = resolve_virtual_path(cwd, &path);
                ensure_not_directory(fs.as_ref(), &target)
                    .await
                    .map_err(|err| format!("Failed to delete file {}: {err}", target.display()))?;
                fs.remove(&target, false)
                    .await
                    .map_err(|err| format!("Failed to delete file {}: {err}", target.display()))?;
                affected.deleted.push(affected_path);
            }
            Hunk::UpdateFile {
                path,
                move_path,
                chunks,
            } => {
                let source = resolve_virtual_path(cwd, &path);
                let original_contents =
                    read_text_file(fs.as_ref(), &source).await.map_err(|err| {
                        format!("Failed to read file to update {}: {err}", source.display())
                    })?;
                let new_contents =
                    derive_new_contents_from_chunks(&source, &original_contents, &chunks)?;

                if let Some(move_path) = move_path {
                    let dest = resolve_virtual_path(cwd, &move_path);
                    write_file_with_missing_parent_retry(
                        fs.as_ref(),
                        &dest,
                        new_contents.into_bytes(),
                    )
                    .await
                    .map_err(|err| format!("Failed to write file {}: {err}", dest.display()))?;
                    ensure_not_directory(fs.as_ref(), &source)
                        .await
                        .map_err(|err| {
                            format!("Failed to remove original {}: {err}", source.display())
                        })?;
                    fs.remove(&source, false).await.map_err(|err| {
                        format!("Failed to remove original {}: {err}", source.display())
                    })?;
                } else {
                    fs.write_file(&source, new_contents.as_bytes())
                        .await
                        .map_err(|err| {
                            format!("Failed to write file {}: {err}", source.display())
                        })?;
                }
                affected.modified.push(affected_path);
            }
        }
    }

    Ok(print_summary(&affected))
}

fn parse_patch(patch: &str) -> Result<Vec<Hunk>, String> {
    let lines = patch.trim().lines().collect::<Vec<_>>();
    let hunk_lines = check_patch_boundaries_lenient(&lines)?;
    let (_environment_id, mut remaining_lines, mut line_number) =
        parse_environment_id_preamble(hunk_lines)?;

    let mut hunks = Vec::new();
    while !remaining_lines.is_empty() {
        let (hunk, parsed_lines) = parse_one_hunk(remaining_lines, line_number)?;
        hunks.push(hunk);
        line_number += parsed_lines;
        remaining_lines = &remaining_lines[parsed_lines..];
    }

    Ok(hunks)
}

fn parse_environment_id_preamble<'a>(
    hunk_lines: &'a [&'a str],
) -> Result<(Option<String>, &'a [&'a str], usize), String> {
    let Some(first_line) = hunk_lines.first() else {
        return Ok((None, hunk_lines, 2));
    };
    let Some(environment_id) = first_line.trim_start().strip_prefix(ENVIRONMENT_ID_MARKER) else {
        return Ok((None, hunk_lines, 2));
    };
    let environment_id = environment_id.trim();
    if environment_id.is_empty() {
        return Err("apply_patch environment_id cannot be empty".to_string());
    }
    Ok((Some(environment_id.to_string()), &hunk_lines[1..], 3))
}

fn check_patch_boundaries_lenient<'a>(lines: &'a [&'a str]) -> Result<&'a [&'a str], String> {
    if check_start_and_end_lines(lines).is_ok() {
        return Ok(&lines[1..lines.len() - 1]);
    }

    match lines {
        [first, .., last]
            if (*first == "<<EOF" || *first == "<<'EOF'" || *first == "<<\"EOF\"")
                && last.ends_with("EOF")
                && lines.len() >= 4 =>
        {
            let inner = &lines[1..lines.len() - 1];
            check_start_and_end_lines(inner)?;
            Ok(&inner[1..inner.len() - 1])
        }
        _ => {
            check_start_and_end_lines(lines)?;
            unreachable!("checked patch boundaries should return earlier")
        }
    }
}

fn check_start_and_end_lines(lines: &[&str]) -> Result<(), String> {
    let first = lines.first().map(|line| line.trim());
    let last = lines.last().map(|line| line.trim());

    match (first, last) {
        (Some(first), Some(last)) if first == BEGIN_PATCH_MARKER && last == END_PATCH_MARKER => {
            Ok(())
        }
        (Some(first), _) if first != BEGIN_PATCH_MARKER => Err(format!(
            "Invalid patch: The first line of the patch must be '{BEGIN_PATCH_MARKER}'"
        )),
        _ => Err(format!(
            "Invalid patch: The last line of the patch must be '{END_PATCH_MARKER}'"
        )),
    }
}

fn parse_one_hunk(lines: &[&str], line_number: usize) -> Result<(Hunk, usize), String> {
    let first_line = lines
        .first()
        .ok_or_else(|| "invalid patch hunk: empty hunk".to_string())?
        .trim();

    if let Some(path) = first_line.strip_prefix(ADD_FILE_MARKER) {
        let mut contents = String::new();
        let mut parsed_lines = 1;
        for add_line in &lines[1..] {
            if let Some(line_to_add) = add_line.strip_prefix('+') {
                contents.push_str(line_to_add);
                contents.push('\n');
                parsed_lines += 1;
            } else {
                break;
            }
        }
        return Ok((
            Hunk::AddFile {
                path: std::path::PathBuf::from(path),
                contents,
            },
            parsed_lines,
        ));
    }

    if let Some(path) = first_line.strip_prefix(DELETE_FILE_MARKER) {
        return Ok((
            Hunk::DeleteFile {
                path: std::path::PathBuf::from(path),
            },
            1,
        ));
    }

    if let Some(path) = first_line.strip_prefix(UPDATE_FILE_MARKER) {
        let mut remaining_lines = &lines[1..];
        let mut parsed_lines = 1;
        let move_path = remaining_lines
            .first()
            .and_then(|line| line.strip_prefix(MOVE_TO_MARKER));
        if move_path.is_some() {
            remaining_lines = &remaining_lines[1..];
            parsed_lines += 1;
        }

        let mut chunks = Vec::new();
        while !remaining_lines.is_empty() {
            if remaining_lines[0].trim().is_empty() {
                parsed_lines += 1;
                remaining_lines = &remaining_lines[1..];
                continue;
            }
            if remaining_lines[0].starts_with('*') {
                break;
            }

            let (chunk, chunk_lines) = parse_update_file_chunk(
                remaining_lines,
                line_number + parsed_lines,
                chunks.is_empty(),
            )?;
            chunks.push(chunk);
            parsed_lines += chunk_lines;
            remaining_lines = &remaining_lines[chunk_lines..];
        }

        if chunks.is_empty() {
            return Err(format!(
                "Invalid patch hunk at line {line_number}: Update file hunk for path '{}' is empty",
                std::path::Path::new(path).display()
            ));
        }

        return Ok((
            Hunk::UpdateFile {
                path: std::path::PathBuf::from(path),
                move_path: move_path.map(std::path::PathBuf::from),
                chunks,
            },
            parsed_lines,
        ));
    }

    Err(format!(
        "Invalid patch hunk at line {line_number}: '{first_line}' is not a valid hunk header"
    ))
}

fn parse_update_file_chunk(
    lines: &[&str],
    line_number: usize,
    allow_missing_context: bool,
) -> Result<(UpdateFileChunk, usize), String> {
    if lines.is_empty() {
        return Err(format!(
            "Invalid patch hunk at line {line_number}: Update hunk does not contain any lines"
        ));
    }

    let (change_context, start_index) = if lines[0] == EMPTY_CHANGE_CONTEXT_MARKER {
        (None, 1)
    } else if let Some(context) = lines[0].strip_prefix(CHANGE_CONTEXT_MARKER) {
        (Some(context.to_string()), 1)
    } else if allow_missing_context {
        (None, 0)
    } else {
        return Err(format!(
            "Invalid patch hunk at line {line_number}: Expected update hunk to start with a @@ context marker, got: '{}'",
            lines[0]
        ));
    };

    if start_index >= lines.len() {
        return Err(format!(
            "Invalid patch hunk at line {}: Update hunk does not contain any lines",
            line_number + 1
        ));
    }

    let mut chunk = UpdateFileChunk {
        change_context,
        old_lines: Vec::new(),
        new_lines: Vec::new(),
        is_end_of_file: false,
    };
    let mut parsed_lines = 0;

    for line in &lines[start_index..] {
        match *line {
            EOF_MARKER => {
                if parsed_lines == 0 {
                    return Err(format!(
                        "Invalid patch hunk at line {}: Update hunk does not contain any lines",
                        line_number + 1
                    ));
                }
                chunk.is_end_of_file = true;
                parsed_lines += 1;
                break;
            }
            line_contents => {
                match line_contents.chars().next() {
                    None => {
                        chunk.old_lines.push(String::new());
                        chunk.new_lines.push(String::new());
                    }
                    Some(' ') => {
                        chunk.old_lines.push(line_contents[1..].to_string());
                        chunk.new_lines.push(line_contents[1..].to_string());
                    }
                    Some('+') => {
                        chunk.new_lines.push(line_contents[1..].to_string());
                    }
                    Some('-') => {
                        chunk.old_lines.push(line_contents[1..].to_string());
                    }
                    _ if parsed_lines > 0 => break,
                    _ => {
                        return Err(format!(
                            "Invalid patch hunk at line {}: Unexpected line found in update hunk: '{}'",
                            line_number + 1,
                            line_contents
                        ));
                    }
                }
                parsed_lines += 1;
            }
        }
    }

    Ok((chunk, parsed_lines + start_index))
}

fn derive_new_contents_from_chunks(
    path: &std::path::Path,
    original_contents: &str,
    chunks: &[UpdateFileChunk],
) -> Result<String, String> {
    let mut original_lines = original_contents
        .split('\n')
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if original_lines.last().is_some_and(String::is_empty) {
        original_lines.pop();
    }

    let replacements = compute_replacements(&original_lines, path, chunks)?;
    let mut new_lines = apply_replacements(original_lines, &replacements);
    if !new_lines.last().is_some_and(String::is_empty) {
        new_lines.push(String::new());
    }
    Ok(new_lines.join("\n"))
}

fn compute_replacements(
    original_lines: &[String],
    path: &std::path::Path,
    chunks: &[UpdateFileChunk],
) -> Result<Vec<(usize, usize, Vec<String>)>, String> {
    let mut replacements = Vec::new();
    let mut line_index = 0;

    for chunk in chunks {
        if let Some(context_line) = &chunk.change_context {
            let Some(index) = seek_sequence(
                original_lines,
                std::slice::from_ref(context_line),
                line_index,
                false,
            ) else {
                return Err(format!(
                    "Failed to find context '{}' in {}",
                    context_line,
                    path.display()
                ));
            };
            line_index = index + 1;
        }

        if chunk.old_lines.is_empty() {
            let insertion_index = if original_lines.last().is_some_and(String::is_empty) {
                original_lines.len() - 1
            } else {
                original_lines.len()
            };
            replacements.push((insertion_index, 0, chunk.new_lines.clone()));
            continue;
        }

        let mut pattern = chunk.old_lines.as_slice();
        let mut found = seek_sequence(original_lines, pattern, line_index, chunk.is_end_of_file);
        let mut new_slice = chunk.new_lines.as_slice();

        if found.is_none() && pattern.last().is_some_and(String::is_empty) {
            pattern = &pattern[..pattern.len() - 1];
            if new_slice.last().is_some_and(String::is_empty) {
                new_slice = &new_slice[..new_slice.len() - 1];
            }
            found = seek_sequence(original_lines, pattern, line_index, chunk.is_end_of_file);
        }

        let Some(start_index) = found else {
            return Err(format!(
                "Failed to find expected lines in {}:\n{}",
                path.display(),
                chunk.old_lines.join("\n")
            ));
        };

        replacements.push((start_index, pattern.len(), new_slice.to_vec()));
        line_index = start_index + pattern.len();
    }

    replacements.sort_by_key(|(index, _, _)| *index);
    Ok(replacements)
}

fn apply_replacements(
    mut lines: Vec<String>,
    replacements: &[(usize, usize, Vec<String>)],
) -> Vec<String> {
    for (start_index, old_len, new_segment) in replacements.iter().rev() {
        for _ in 0..*old_len {
            if *start_index < lines.len() {
                lines.remove(*start_index);
            }
        }
        for (offset, new_line) in new_segment.iter().enumerate() {
            lines.insert(*start_index + offset, new_line.clone());
        }
    }
    lines
}

fn seek_sequence(lines: &[String], pattern: &[String], start: usize, eof: bool) -> Option<usize> {
    if pattern.is_empty() {
        return Some(start);
    }
    if pattern.len() > lines.len() {
        return None;
    }

    let search_start = if eof && lines.len() >= pattern.len() {
        lines.len() - pattern.len()
    } else {
        start
    };

    for index in search_start..=lines.len().saturating_sub(pattern.len()) {
        if lines[index..index + pattern.len()] == *pattern {
            return Some(index);
        }
    }

    for index in search_start..=lines.len().saturating_sub(pattern.len()) {
        if pattern
            .iter()
            .enumerate()
            .all(|(offset, expected)| lines[index + offset].trim_end() == expected.trim_end())
        {
            return Some(index);
        }
    }

    for index in search_start..=lines.len().saturating_sub(pattern.len()) {
        if pattern
            .iter()
            .enumerate()
            .all(|(offset, expected)| lines[index + offset].trim() == expected.trim())
        {
            return Some(index);
        }
    }

    for index in search_start..=lines.len().saturating_sub(pattern.len()) {
        if pattern.iter().enumerate().all(|(offset, expected)| {
            normalize_punctuation(&lines[index + offset]) == normalize_punctuation(expected)
        }) {
            return Some(index);
        }
    }

    None
}

fn normalize_punctuation(value: &str) -> String {
    value
        .trim()
        .chars()
        .map(|ch| match ch {
            '\u{2010}' | '\u{2011}' | '\u{2012}' | '\u{2013}' | '\u{2014}' | '\u{2015}'
            | '\u{2212}' => '-',
            '\u{2018}' | '\u{2019}' | '\u{201A}' | '\u{201B}' => '\'',
            '\u{201C}' | '\u{201D}' | '\u{201E}' | '\u{201F}' => '"',
            '\u{00A0}' | '\u{2002}' | '\u{2003}' | '\u{2004}' | '\u{2005}' | '\u{2006}'
            | '\u{2007}' | '\u{2008}' | '\u{2009}' | '\u{200A}' | '\u{202F}' | '\u{205F}'
            | '\u{3000}' => ' ',
            other => other,
        })
        .collect()
}

async fn ensure_not_directory(
    fs: &dyn bashkit::FileSystem,
    path: &std::path::Path,
) -> bashkit::Result<()> {
    let metadata = fs.stat(path).await?;
    if metadata.file_type.is_dir() {
        return Err(
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path is a directory").into(),
        );
    }
    Ok(())
}

async fn write_file_with_missing_parent_retry(
    fs: &dyn bashkit::FileSystem,
    path: &std::path::Path,
    contents: Vec<u8>,
) -> bashkit::Result<()> {
    match fs.write_file(path, &contents).await {
        Ok(()) => Ok(()),
        Err(err) if error_kind(&err) == Some(std::io::ErrorKind::NotFound) => {
            if let Some(parent) = path.parent() {
                fs.mkdir(parent, true).await?;
            }
            fs.write_file(path, &contents).await
        }
        Err(err) => Err(err),
    }
}

async fn read_text_file(
    fs: &dyn bashkit::FileSystem,
    path: &std::path::Path,
) -> bashkit::Result<String> {
    let bytes = fs.read_file(path).await?;
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

fn print_summary(affected: &AffectedPaths) -> String {
    let mut out = String::from("Success. Updated the following files:\n");
    for path in &affected.added {
        out.push_str(&format!("A {}\n", path.display()));
    }
    for path in &affected.modified {
        out.push_str(&format!("M {}\n", path.display()));
    }
    for path in &affected.deleted {
        out.push_str(&format!("D {}\n", path.display()));
    }
    out
}

fn resolve_virtual_path(cwd: &std::path::Path, path: &std::path::Path) -> std::path::PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    bashkit::normalize_path(&joined)
}

fn error_kind(err: &bashkit::Error) -> Option<std::io::ErrorKind> {
    match err {
        bashkit::Error::Io(source) => Some(source.kind()),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
