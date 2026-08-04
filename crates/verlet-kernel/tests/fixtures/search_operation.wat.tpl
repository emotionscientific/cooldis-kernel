(; Intentionally WAT: this probes the raw HTTP ABI import wiring with a
   dynamically injected mock-server URL. Tool-like guest examples should prefer
   Rust crates under tests/fixtures/. ;)
(module
  (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
  (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
  (import "cooldis_0.1" "event_emit" (func $event_emit (param i32 i32 i32) (result i32)))
  (import "cooldis_0.1" "http_request" (func $http_request (param i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 4096) "{{manifest}}")
  (data (i32.const 8192) "{{request}}")
  (data (i32.const 12288) "{{body}}")
  (func (export "__verlet_describe_module__") (param $sink i32) (result i32)
    i32.const 0
    i32.const {{manifest_len}}
    i32.store
    local.get $sink
    i32.const 4096
    i32.const 0
    call $sink_write)
  (func (export "__verlet_call_operation__")
    (param $op i32)
    (param $invocation i32)
    (param $source i32)
    (param $output i32)
    (param $events i32)
    (result i32)
    (local $status i32)
    (local $meta_source i32)
    (local $body_source i32)
    (local $n i32)
    local.get $op
    i32.const 1
    i32.ne
    if
      i32.const 2
      return
    end
    local.get $invocation
    i32.const 8192
    i32.const {{request_len}}
    i32.const 12288
    i32.const {{body_len}}
    i32.const 64
    local.get $events
    call $http_request
    local.set $status
    local.get $status
    i32.const 0
    i32.ne
    if
      local.get $status
      return
    end
    i32.const 64
    i32.load
    local.set $meta_source
    i32.const 68
    i32.load
    local.set $body_source
    i32.const 0
    i32.const 2048
    i32.store
    local.get $meta_source
    i32.const 16384
    i32.const 0
    call $source_read
    drop
    i32.const 0
    i32.load
    local.set $n
    i32.const 0
    local.get $n
    i32.store
    local.get $invocation
    i32.const 16384
    i32.const 0
    call $event_emit
    drop
    i32.const 0
    i32.const 2048
    i32.store
    local.get $body_source
    i32.const 20480
    i32.const 0
    call $source_read
    drop
    i32.const 0
    i32.load
    local.set $n
    i32.const 0
    local.get $n
    i32.store
    local.get $output
    i32.const 20480
    i32.const 0
    call $sink_write
    drop
    i32.const 0))
