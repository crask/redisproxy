#!/bin/bash
set -e

err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
}

# we only sync master branch when built from gitlab-ci
if [[ "${CI_BUILD_REF_NAME}" != "master" ]]; then
    echo "Skip branch ${CI_BUILD_REF_NAME}"
    exit 0
fi

username=""
password=""
include_files=""

svn_repo="$1"
if [[ -z "$svn_repo" ]]; then
    err "Please specify svn repo url"
    exit 1
fi

shift

while getopts 'u:p:i:' flag; do
    case "${flag}" in
        u) username="${OPTARG}" ;;
        p) password="${OPTARG}" ;;
        i) include_files="${include_files} ${OPTARG}" ;;
        *) err "Unexpected option ${flag}"; exit 1 ;;
        
    esac
done

if [[ -z "${username}" ]]; then
    err "Please specify svn username"
    exit 1
fi

if [[ -z "${password}" ]]; then
    err "Please specify svn password"
    exit 1
fi


if [ -d .git/svn ]; then
    rm -rf .git/svn
fi

commit_message="$(git log --oneline HEAD -n 1)"

svn co --non-interactive --trust-server-cert \
    --username "${username}" --password "${password}" "${svn_repo}" .git/svn

git ls-files | xargs -i{} rsync -R {} .git/svn

for file in "${include_files}"; do
    rsync -R $file .git/svn
done

cd .git/svn

svn add --force .

svn ci --non-interactive --trust-server-cert \
    --username "${username}" --password "${password}" \
    -m "${commit_message}"
