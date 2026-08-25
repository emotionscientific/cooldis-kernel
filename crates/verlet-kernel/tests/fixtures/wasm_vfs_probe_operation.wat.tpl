(; Intentionally WAT: this fixture pokes low-level VFS status codes and handle
   behavior at the raw host ABI boundary. Normal tool fixtures use Rust guests. ;)
(module
  (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
  (import "cooldis_0.1" "fs_open" (func $fs_open (param i32 i32 i32 i32) (result i32)))
  (import "cooldis_0.1" "fs_read" (func $fs_read (param i32 i32 i32) (result i32)))
  (import "cooldis_0.1" "fs_close" (func $fs_close (param i32) (result i32)))
  (import "cooldis_0.1" "fs_write" (func $fs_write (param i32 i32 i32) (result i32)))
  (import "cooldis_0.1" "fs_stat" (func $fs_stat (param i32 i32 i32) (result i32)))
  (import "cooldis_0.1" "fs_mkdir" (func $fs_mkdir (param i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 4096) "{{manifest}}")
  (data (i32.const 8192) "{{path}}")
  (data (i32.const 8256) "{{pending_path}}")
  (data (i32.const 8320) "{{recursive_path}}")
  (data (i32.const 8384) "{{nonrecursive_path}}")
  (data (i32.const 8448) "{{missing_parent_path}}")
  (data (i32.const 8512) "pending")
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
    (local $handle i32)
    local.get $op
    i32.const 1
    i32.eq
    if
      i32.const 8192
      i32.const {{path_len}}
      i32.const {{invalid_mode}}
      i32.const 64
      call $fs_open
      return
    end
    local.get $op
    i32.const 2
    i32.eq
    if
      i32.const 0
      i32.const 4
      i32.store
      i32.const 9999
      i32.const 2048
      i32.const 0
      call $fs_read
      return
    end
    local.get $op
    i32.const 3
    i32.eq
    if
      i32.const 8192
      i32.const {{path_len}}
      i32.const {{read_mode}}
      i32.const 64
      call $fs_open
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
      local.set $handle
      local.get $handle
      call $fs_close
      local.set $status
      local.get $status
      i32.const 0
      i32.ne
      if
        local.get $status
        return
      end
      local.get $handle
      call $fs_close
      return
    end
    local.get $op
    i32.const 4
    i32.eq
    if
      i32.const 8256
      i32.const {{pending_path_len}}
      i32.const {{write_mode}}
      i32.const 64
      call $fs_open
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
      i32.const 8512
      i32.const 7
      call $fs_write
      return
    end
    local.get $op
    i32.const 5
    i32.eq
    if
      i32.const 8320
      i32.const {{recursive_path_len}}
      i32.const 1
      call $fs_mkdir
      return
    end
    local.get $op
    i32.const 6
    i32.eq
    if
      i32.const 8384
      i32.const {{nonrecursive_path_len}}
      i32.const 0
      call $fs_mkdir
      return
    end
    local.get $op
    i32.const 7
    i32.eq
    if
      i32.const 8192
      i32.const {{path_len}}
      i32.const {{read_mode}}
      i32.const 64
      call $fs_open
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
      i32.const 8512
      i32.const 7
      call $fs_write
      return
    end
    local.get $op
    i32.const 8
    i32.eq
    if
      i32.const 8256
      i32.const {{pending_path_len}}
      i32.const {{write_mode}}
      i32.const 64
      call $fs_open
      local.set $status
      local.get $status
      i32.const 0
      i32.ne
      if
        local.get $status
        return
      end
      i32.const 0
      i32.const 4
      i32.store
      i32.const 64
      i32.load
      i32.const 2048
      i32.const 0
      call $fs_read
      return
    end
    local.get $op
    i32.const 9
    i32.eq
    if
      i32.const 9999
      i32.const 8512
      i32.const 7
      call $fs_write
      return
    end
    local.get $op
    i32.const 10
    i32.eq
    if
      i32.const 8192
      i32.const {{path_len}}
      i32.const 128
      call $fs_stat
      local.set $status
      local.get $status
      i32.const 0
      i32.ne
      if
        local.get $status
        return
      end
      i32.const 0
      i32.const 16
      i32.store
      local.get $output
      i32.const 128
      i32.const 0
      call $sink_write
      return
    end
    local.get $op
    i32.const 11
    i32.eq
    if
      i32.const 8448
      i32.const {{missing_parent_path_len}}
      i32.const 0
      call $fs_mkdir
      return
    end
    local.get $op
    i32.const 12
    i32.eq
    if
      i32.const 8256
      i32.const {{pending_path_len}}
      i32.const {{write_mode}}
      i32.const 64
      call $fs_open
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
      local.set $handle
      local.get $handle
      i32.const 8512
      i32.const 7
      call $fs_write
      local.set $status
      local.get $status
      i32.const 0
      i32.ne
      if
        local.get $status
        return
      end
      local.get $handle
      i32.const 65530
      i32.const 20
      call $fs_write
      local.set $status
      local.get $status
      i32.const {{invalid_argument}}
      i32.ne
      if
        unreachable
      end
      local.get $handle
      call $fs_close
      return
    end
    local.get $op
    i32.const 13
    i32.eq
    if
      i32.const 8448
      i32.const {{missing_parent_path_len}}
      i32.const {{write_mode}}
      i32.const 64
      call $fs_open
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
      local.set $handle
      local.get $handle
      i32.const 8512
      i32.const 7
      call $fs_write
      local.set $status
      local.get $status
      i32.const 0
      i32.ne
      if
        local.get $status
        return
      end
      local.get $handle
      call $fs_close
      local.set $status
      local.get $status
      i32.const {{not_found}}
      i32.ne
      if
        local.get $status
        return
      end
      i32.const 8448
      i32.const {{missing_parent_dir_len}}
      i32.const 0
      call $fs_mkdir
      local.set $status
      local.get $status
      i32.const 0
      i32.ne
      if
        local.get $status
        return
      end
      local.get $handle
      call $fs_close
      return
    end
    local.get $op
    i32.const 14
    i32.eq
    if
      i32.const 8320
      i32.const {{recursive_path_len}}
      i32.const 2
      call $fs_mkdir
      return
    end
    i32.const {{not_found}}))
