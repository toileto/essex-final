#/bin/bash
set -e

# Get commit hash
commit_sha_check=$(eval git rev-parse HEAD)
commit_sha_dynamic=${CI_COMMIT_SHA:-$commit_sha_direct}
commit_unixts_sec=$(eval git show -s --format=%ct $commit_sha_dynamic)
release_version_result=$commit_unixts_sec
printf $release_version_result