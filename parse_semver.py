import argparse
import sys
import json
import semver

def main():
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="A command-line tool to parse SemVer strings using the 'semver' library."
    )
    
    # Add the positional argument for the version string
    parser.add_argument(
        "version", 
        type=str, 
        help="The SemVer string to parse (e.g., 1.2.3-alpha.1+build.123)"
    )
    
    # Parse the arguments from the command line
    args = parser.parse_args()
    
    try:
        # Parse the version using the semver library
        parsed_version = semver.VersionInfo.parse(args.version)
        
        # Construct a dictionary to easily output as JSON
        result = {
            "major": parsed_version.major,
            "minor": parsed_version.minor,
            "patch": parsed_version.patch,
            "prerelease": parsed_version.prerelease,
            "build": parsed_version.build
        }
        
        # Print the result nicely formatted
        print(json.dumps(result, indent=4))
        
    except ValueError as e:
        # The semver library raises a ValueError if the string is invalid
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
