#!/bin/bash

# Base directory for integrations
INTEGRATIONS_DIR="integrations"

# Function to extract information from Go files and generate markdown
generate_md_from_go() {
    local dir=$1
    local output_file=$2

    echo "# Integration: $(basename $dir)" > "$output_file"
    echo "" >> "$output_file"
    echo "## Overview" >> "$output_file"
    echo "This page summarizes the Go files found in the $(basename $dir) integration." >> "$output_file"
    echo "" >> "$output_file"

    for go_file in "$dir"/*.go; do
        if [[ -f $go_file ]]; then
            echo "### $(basename $go_file)" >> "$output_file"
            echo "" >> "$output_file"

            # Extract package name
            package_name=$(awk '/^package / {print $2}' "$go_file")
            if [[ -n $package_name ]]; then
                echo "- **Package**: \`$package_name\`" >> "$output_file"
            fi

            # Extract imports
            imports=$(awk '/^import /,/^)/' "$go_file" | sed '/^import /d' | tr -d '()')
            if [[ -n $imports ]]; then
                echo "- **Imports**:" >> "$output_file"
                echo '```go' >> "$output_file"
                echo "$imports" >> "$output_file"
                echo '```' >> "$output_file"
            fi

            # Extract functions
            functions=$(awk '/^func / {print $0}' "$go_file")
            if [[ -n $functions ]]; then
                echo "- **Functions**:" >> "$output_file"
                echo '```go' >> "$output_file"
                echo "$functions" >> "$output_file"
                echo '```' >> "$output_file"
            fi

            echo "" >> "$output_file"
        fi
    done
}

# Function to process a directory and generate markdown
process_directory() {
    local dir=$1
    local output_dir=$2

    mkdir -p "$output_dir"
    output_file="$output_dir/$(basename $output_dir).md"
    generate_md_from_go "$dir" "$output_file"
    echo "Generated $output_file"
}

# Iterate over each directory in integrations
for dir in "$INTEGRATIONS_DIR"/*; do
    if [[ -d $dir ]]; then
        if [[ $(basename $dir) == "api" ]]; then
            # Special case for api subdirectories
            for sub_dir in "$dir"/*; do
                if [[ -d $sub_dir ]]; then
                    output_dir="$sub_dir"
                    process_directory "$sub_dir" "$output_dir"
                fi
            done
        else
            # Regular case for other integrations
            output_dir="$dir"
            process_directory "$dir" "$output_dir"
        fi
    fi
done
