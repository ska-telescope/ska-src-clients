#!/bin/sh
# Increment the application version.

IFS=. read -r major minor patch < ../../VERSION

case "$1" in
patch) tag="$major.$minor.$((patch+1))"; ;;
major) tag="$((major+1)).0.0"; ;;
minor)     tag="$major.$((minor+1)).0"; ;;
esac

# Change the application VERSION

echo $tag > ../../VERSION

# Change the helm chart application version
yq w -i ../helm/Chart.yaml appVersion $tag

