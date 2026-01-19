#!/bin/bash
set -e

# Release script for puffgres
# Creates a git tag and pushes it, which triggers the GitHub Actions release workflow

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get current version from Cargo.toml
get_current_version() {
    grep -m1 'version = "' "$ROOT_DIR/Cargo.toml" | sed 's/.*version = "\([^"]*\)".*/\1/'
}

# Bump version
bump_version() {
    local version=$1
    local part=$2

    IFS='.' read -r major minor patch <<< "$version"

    case $part in
        major)
            echo "$((major + 1)).0.0"
            ;;
        minor)
            echo "$major.$((minor + 1)).0"
            ;;
        patch)
            echo "$major.$minor.$((patch + 1))"
            ;;
        *)
            echo "$version"
            ;;
    esac
}

# Update version in all relevant files
update_version() {
    local new_version=$1

    echo -e "${YELLOW}Updating version to $new_version...${NC}"

    # Update workspace Cargo.toml
    sed -i '' "s/^version = \"[^\"]*\"/version = \"$new_version\"/" "$ROOT_DIR/Cargo.toml"

    # Update npm/package.json
    cd "$ROOT_DIR/npm"
    npm version "$new_version" --no-git-tag-version --allow-same-version
    cd "$ROOT_DIR"

    echo -e "${GREEN}Updated version in Cargo.toml and npm/package.json${NC}"
}

# Main
main() {
    cd "$ROOT_DIR"

    current_version=$(get_current_version)
    echo -e "Current version: ${GREEN}$current_version${NC}"

    # Parse arguments
    case "${1:-}" in
        major|minor|patch)
            new_version=$(bump_version "$current_version" "$1")
            ;;
        v*)
            new_version="${1#v}"
            ;;
        [0-9]*)
            new_version="$1"
            ;;
        "")
            echo ""
            echo "Usage: $0 <version|major|minor|patch>"
            echo ""
            echo "Examples:"
            echo "  $0 patch        # $current_version -> $(bump_version "$current_version" patch)"
            echo "  $0 minor        # $current_version -> $(bump_version "$current_version" minor)"
            echo "  $0 major        # $current_version -> $(bump_version "$current_version" major)"
            echo "  $0 0.2.0        # Set explicit version"
            echo "  $0 v0.2.0       # Set explicit version (v prefix stripped)"
            echo ""
            exit 1
            ;;
        *)
            echo -e "${RED}Invalid argument: $1${NC}"
            exit 1
            ;;
    esac

    echo -e "New version: ${GREEN}$new_version${NC}"
    echo ""

    # Confirm
    read -p "Continue with release v$new_version? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 1
    fi

    # Check for uncommitted changes
    if [[ -n $(git status --porcelain) ]]; then
        echo -e "${YELLOW}Warning: You have uncommitted changes.${NC}"
        read -p "Continue anyway? [y/N] " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Aborted."
            exit 1
        fi
    fi

    # Update version
    update_version "$new_version"

    # Commit version bump
    git add -A
    git commit -m "chore: release v$new_version"

    # Create and push tag
    echo -e "${YELLOW}Creating tag v$new_version...${NC}"
    git tag -a "v$new_version" -m "Release v$new_version"

    echo -e "${YELLOW}Pushing to origin...${NC}"
    git push origin main
    git push origin "v$new_version"

    echo ""
    echo -e "${GREEN}Release v$new_version created and pushed!${NC}"
    echo ""
    echo "GitHub Actions will now:"
    echo "  1. Build binaries for all platforms"
    echo "  2. Create a GitHub Release with the binaries"
    echo "  3. Update the Homebrew formula"
    echo ""
    echo "Watch progress at: https://github.com/lucasgelfond/puffgres/actions"
}

main "$@"
