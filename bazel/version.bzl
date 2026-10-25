"""The version of the library.

On main it stays at the development value; the release workflow rewrites this
file from the Git tag (scripts/set-version.sh) before it builds, and does not
commit the change.
"""

VERSION = "0.0.0-dev"
