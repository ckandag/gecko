#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

render() {
	printf '%s' "$1" > "$tmp_dir/labels.json"
	helm template test "$chart_dir" --set image.tag=test \
		--set-file "customerLabels=$tmp_dir/labels.json" > "$tmp_dir/rendered.yaml" 2> "$tmp_dir/error"
}

render '{}'
if grep -q 'customer-labels.json' "$tmp_dir/rendered.yaml"; then
	echo 'empty labels unexpectedly created a labels ConfigMap or mount' >&2
	exit 1
fi

render '{"team":"example"}'
grep -q '"team": "example"' "$tmp_dir/rendered.yaml"
grep -q -- '--customer-labels-file=' "$tmp_dir/rendered.yaml"

for invalid in '{bad' '["team"]' '"team"' '42' 'true'; do
	if render "$invalid"; then
		echo "customerLabels accepted invalid JSON or a non-object: $invalid" >&2
		exit 1
	fi
done

echo 'Customer label chart tests passed.'
