#!/usr/bin/env bash


for file in $(git diff --name-only origin/master | grep -e '\.py$'); do
    ~/.local/bin/futurize --stage1 "$file" -o ~/py3-files -nw >> ~/py3.diff
    while read fixer; do
        if [ ! -z "$fixer" ]
        then
            ~/.local/bin/futurize -f "$fixer" "$file" -o ~/py3-files -nw >> ~/py3.diff
        fi
    done <.circleci/check_fixers
done

if [ -s ~/py3.diff ]
then
    cat ~/py3.diff
    exit 1
fi
