#!/bin/bash
#
# Builds the DocC documentation and serves it locally for previewing.
#
# By default this builds a combined archive covering every documented target, which is what
# .github/workflows/docs.yml publishes to GitHub Pages: a landing page at /documentation/ that
# links to each module, and cross-module links between them.
#
# Pass --live to use the Swift-DocC preview server instead, which rebuilds as you edit. DocC can
# only preview one target at a time, so that mode needs a single --target.
#
set -euo pipefail

DEFAULT_TARGETS=(RBDB Datalog)
TARGETS=()
PORT="8080"
LIVE=0
OUT_DIR=".build/docs"

usage() {
	cat <<-EOF
		Usage: $(basename "$0") [options]

		Options:
		  -t, --target NAME   Target to document; repeatable
		                      (default: ${DEFAULT_TARGETS[*]})
		  -l, --live          Use the DocC preview server, which rebuilds as you edit.
		                      Requires exactly one --target.
		  -p, --port PORT     Port to serve on (default: $PORT)
		  -o, --output DIR    Output directory (default: $OUT_DIR)
		  -h, --help          Show this help
	EOF
}

while [ $# -gt 0 ]; do
	case "$1" in
		-t | --target)
			TARGETS+=("$2")
			shift 2
			;;
		-l | --live)
			LIVE=1
			shift
			;;
		-p | --port)
			PORT="$2"
			shift 2
			;;
		-o | --output)
			OUT_DIR="$2"
			shift 2
			;;
		-h | --help)
			usage
			exit 0
			;;
		*)
			echo "Unknown option: $1" >&2
			usage >&2
			exit 1
			;;
	esac
done

if [ ${#TARGETS[@]} -eq 0 ]; then
	TARGETS=("${DEFAULT_TARGETS[@]}")
fi

cd "$(dirname "$0")/.."

# Expand the targets into the repeated `--target` arguments the plugin expects.
TARGET_ARGS=()
for target in "${TARGETS[@]}"; do
	TARGET_ARGS+=(--target "$target")
done

if [ "$LIVE" -eq 1 ]; then
	if [ ${#TARGETS[@]} -ne 1 ]; then
		echo "Error: --live can only preview one target at a time, but ${#TARGETS[@]} were given." >&2
		echo "Pick one, e.g. $(basename "$0") --live --target ${TARGETS[0]}" >&2
		exit 1
	fi

	# Lowercased target name, which is what DocC uses in the documentation URL.
	target_path="$(echo "${TARGETS[0]}" | tr '[:upper:]' '[:lower:]')"
	echo "Previewing ${TARGETS[0]} docs at http://localhost:$PORT/documentation/$target_path/"

	# --disable-sandbox is required: the preview server writes to and serves from a directory
	# outside the package.
	exec swift package --disable-sandbox preview-documentation \
		"${TARGET_ARGS[@]}" \
		--port "$PORT"
fi

mkdir -p "$OUT_DIR"

# The combined archive is what gives us a single site with a landing page linking each module.
# It's still gated behind an experimental flag, but is what the Pages workflow publishes too.
swift package \
	--allow-writing-to-directory "$OUT_DIR" \
	generate-documentation \
	"${TARGET_ARGS[@]}" \
	--enable-experimental-combined-documentation \
	--disable-indexing \
	--transform-for-static-hosting \
	--output-path "$OUT_DIR"

echo
echo "Serving $OUT_DIR at http://localhost:$PORT/documentation/"
echo "Press Ctrl-C to stop."
exec python3 -m http.server "$PORT" --directory "$OUT_DIR"
