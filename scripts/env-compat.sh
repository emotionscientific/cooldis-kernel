#!/bin/sh

# Resolve a canonical VERLET_* variable, falling back to its one-release
# predecessor. Results are returned through VERLET_ENV_VALUE and
# VERLET_ENV_IS_SET so callers do not need command substitutions.
VERLET_ENV_WARNED=${VERLET_ENV_WARNED-}

verlet_env_read() {
  canonical=$1
  case "$canonical" in
    VERLET_[A-Z0-9_]*) ;;
    *)
      printf 'error: invalid Verlet environment name: %s\n' "$canonical" >&2
      return 2
      ;;
  esac

  eval "canonical_is_set=\${$canonical+x}"
  if [ "$canonical_is_set" = x ]; then
    eval "VERLET_ENV_VALUE=\${$canonical}"
    VERLET_ENV_IS_SET=1
    return 0
  fi

  suffix=${canonical#VERLET_}
  legacy="COOL""DIS_$suffix"
  eval "legacy_is_set=\${$legacy+x}"
  if [ "$legacy_is_set" = x ]; then
    eval "VERLET_ENV_VALUE=\${$legacy}"
    VERLET_ENV_IS_SET=1
    case ":$VERLET_ENV_WARNED:" in
      *":$legacy:"*) ;;
      *)
        printf 'warning: %s is deprecated; use %s (compatibility will be removed in v0.4.0)\n' \
          "$legacy" "$canonical" >&2
        VERLET_ENV_WARNED="${VERLET_ENV_WARNED:+$VERLET_ENV_WARNED:}$legacy"
        ;;
    esac
    return 0
  fi

  VERLET_ENV_VALUE=
  VERLET_ENV_IS_SET=0
}

verlet_env_promote() {
  canonical=$1
  verlet_env_read "$canonical"
  if [ "$VERLET_ENV_IS_SET" = 1 ]; then
    export "$canonical=$VERLET_ENV_VALUE"
  fi
}
