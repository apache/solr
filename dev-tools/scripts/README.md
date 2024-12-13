# Developer Scripts

This folder contains various useful scripts for developers, mostly related to
releasing new versions of Solr and testing those.

Python scripts require Python 3.6 or above. To install necessary python modules, please run:

    pip3 install -r requirements.txt

## Scripts description

### smokeTestRelease.py

Used to validate new release candidates (RC). The script downloads an RC from a URL
or local folder, then runs a number of sanity checks on the artifacts, and then runs
the full tests.

    usage: smokeTestRelease.py [-h] [--tmp-dir PATH] [--not-signed]
                               [--local-keys PATH] [--revision REVISION]
                               [--version X.Y.Z(-ALPHA|-BETA)?]
                               [--test-java12 JAVA12_HOME] [--download-only]
                               url ...
    
    Utility to test a release.
    
    positional arguments:
      url                   Url pointing to release to test
      test_args             Arguments to pass to ant for testing, e.g.
                            -Dwhat=ever.
    
    optional arguments:
      -h, --help            show this help message and exit
      --tmp-dir PATH        Temporary directory to test inside, defaults to
                            /tmp/smoke_solr_$version_$revision
      --not-signed          Indicates the release is not signed
      --local-keys PATH     Uses local KEYS file instead of fetching from
                            https://archive.apache.org/dist/solr/KEYS
      --revision REVISION   GIT revision number that release was built with,
                            defaults to that in URL
      --version X.Y.Z(-ALPHA|-BETA)?
                            Version of the release, defaults to that in URL
      --test-java12 JAVA12_HOME
                            Path to Java12 home directory, to run tests with if
                            specified
      --download-only       Only perform download and sha hash check steps
    
    Example usage:
    python3 -u dev-tools/scripts/smokeTestRelease.py https://dist.apache.org/repos/dist/dev/solr/solr-9.0.1-RC2-revc7510a0...

### releaseWizard.py

The Release Wizard guides the Release Manager through the release process step 
by step, helping you to to run the right commands in the right order, generating
e-mail templates with the correct texts, versions, paths etc, obeying
the voting rules and much more. It also serves as a documentation of all the
steps, with timestamps, preserving log files from each command etc, showing only
the steps and commands required for a major/minor/bugfix release. It also lets
you generate a full Asciidoc guide for the release. The wizard will execute many 
of the other tools in this folder. 

    usage: releaseWizard.py [-h] [--dry-run] [--init]
    
    Script to guide a RM through the whole release process
    
    optional arguments:
      -h, --help  show this help message and exit
      --dry-run   Do not execute any commands, but echo them instead. Display
                  extra debug info
      --init      Re-initialize root and version
    
    Go push that release!

### buildAndPushRelease.py

    usage: buildAndPushRelease.py [-h] [--no-prepare] [--local-keys PATH] [--push-local PATH] [--sign FINGERPRINT]
    [--sign-method-gradle] [--gpg-pass-noprompt] [--gpg-home PATH] [--rc-num NUM]
    [--root PATH] [--logfile PATH] [--dev-mode]
    
    Utility to build, push, and test a release.
    
    optional arguments:
    -h, --help            show this help message and exit
    --no-prepare          Use the already built release in the provided checkout
    --local-keys PATH     Uses local KEYS file to validate presence of RM's gpg key
    --push-local PATH     Push the release to the local path
    --sign FINGERPRINT    Sign the release with the given gpg key. This must be the full GPG fingerprint, not just the
                          last 8 characters.
    --sign-method-gradle  Use Gradle built-in GPG signing instead of gpg command for signing artifacts. This may require
    --gpg-secring argument if your keychain cannot be resolved automatically.
    --gpg-pass-noprompt   Do not prompt for gpg passphrase. For the default gnupg method, this means your gpg-agent needs
                          a non-TTY pin-entry program. For gradle signing method, passphrase must be provided in
                          gradle.properties or by env.var/sysprop. See ./gradlew helpPublishing for more info
    --gpg-home PATH       Path to gpg home containing your secring.gpg Optional, will use $HOME/.gnupg/secring.gpg by
                          default
    --rc-num NUM          Release Candidate number. Default: 1
    --root PATH           Root of Git working tree for solr. Default: "." (the current directory)
    --logfile PATH        Specify log file path (default /tmp/release.log)
    --dev-mode            Enable development mode, which disables some strict checks
    
    Example usage for a Release Manager:
    python3 -u dev-tools/scripts/buildAndPushRelease.py --push-local /tmp/releases/6.0.1 --sign 3782CBB60147010B330523DD26FBCC7836BF353A --rc-num 1

### addVersion.py

    usage: addVersion.py [-h] [-l LUCENE_VERSION] version
    
    Add a new version to CHANGES, to Version.java, build.gradle and solrconfig.xml files
    
    positional arguments:
      version            New Solr version
    
    optional arguments:
      -h, --help         show this help message and exit
      -l LUCENE_VERSION  Optional lucene version. By default will read versions.props

### releasedJirasRegex.py

Pulls out all JIRAs mentioned at the beginning of bullet items
under the given version in the given CHANGES.txt file
and prints a regular expression that will match all of them

    usage: releasedJirasRegex.py [-h] version changes
    
    Prints a regex matching JIRAs fixed in the given version by parsing the given
    CHANGES.txt file
    
    positional arguments:
      version     Version of the form X.Y.Z
      changes     CHANGES.txt file to parse
    
    optional arguments:
      -h, --help  show this help message and exit

### reproduceJenkinsFailures.py

    usage: reproduceJenkinsFailures.py [-h] [--no-git] [--iters N] URL
    
    Must be run from a Solr git workspace. Downloads the Jenkins
    log pointed to by the given URL, parses it for Git revision and failed
    Solr tests, checks out the Git revision in the local workspace,
    groups the failed tests by module, then runs
    'ant test -Dtest.dups=%d -Dtests.class="*.test1[|*.test2[...]]" ...'
    in each module of interest, failing at the end if any of the runs fails.
    To control the maximum number of concurrent JVMs used for each module's
    test run, set 'tests.jvms', e.g. in ~/lucene.build.properties
    
    positional arguments:
      URL         Points to the Jenkins log to parse
    
    optional arguments:
      -h, --help  show this help message and exit
      --no-git    Do not run "git" at all
      --iters N   Number of iterations per test suite (default: 5)

### githubPRs.py

    usage: githubPRs.py [-h] [--json] [--token TOKEN]
    
    Find open Pull Requests that need attention
    
    optional arguments:
      -h, --help     show this help message and exit
      --json         Output as json
      --token TOKEN  Github access token in case you query too often anonymously

### scaffoldNewModule.py

Scaffold a new module and include it into the build. It will set up the folders
and all for you, so the only thing you need to do is add classes, tests and test-data.

    usage: scaffoldNewModule.py [-h] name full_name description
    
    Scaffold new module into solr/modules/<name>
    
    positional arguments:
        name         code-name/id, e.g. my-module
        full_name    Readable name, e.g. "My Module"
        description  Short description for docs
    
    optional arguments:
     -h, --help   show this help message and exit

    Example: ./scaffoldNewModule.py foo "My Module" "Very Useful module here"

### gitignore-gen.sh

TBD


### cherrypick.sh

    Usage: dev-tools/scripts/cherrypick.sh [<options>] <commit-hash> [<commit-hash>...]
     -b <branch> Sets the branch(es) to cherry-pick to, typically branch_Nx or branch_x_y
     -s          Skips precommit test. WARNING: Always run precommit for code- and doc changes
     -t          Run the full test suite during check, not only precommit
     -n          Skips git pull of target branch. Useful if you are without internet access
     -a          Enters automated mode. Aborts cherry-pick and exits on error
     -r <remote> Specify remote to push to. Defaults to 'origin'
     -p          Push to remote. Only done if both cherry-pick and tests succeeded
     WARNING: Never push changes to a remote branch before a thorough local test
    
    Simple script for aiding in back-porting one or more (trivial) commits to other branches.
    On merge conflict the script will run 'git mergetool'. See 'git mergetool --help'
    for help on configuring your favourite merge tool. Check out Sublime Merge (smerge).
    
    Example:
      # Backport two commits to both stable and release branches
      dev-tools/scripts/cherrypick.sh -b branch_9x -b branch_9_0 deadbeef0000 cafebabe1111
