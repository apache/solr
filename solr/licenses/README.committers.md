# Licensing Information for Committers

When a new dependency is added to Solr (or an existing dependency is upgraded),
the Solr project stores metadata about that dependency to meet various practical
and legal requirements.  This metadata consists mainly of "checksum", "license",
and "NOTICE" files.

Checksum files are used by the build to validate downloaded JARs prior to
packaging.  License and notice files meet the legal obligations that arise from
using each dependency.  License files contain the license of each dependency;
"NOTICE" files store attribution or other information that the copyright holder
of a dependency wants included.  License and Notice files are included in this
directory for each dependency, and project-wide licensing and notice information
is included in Solr's LICENSE.txt and NOTICE.txt file in the parent directory.

Under no circumstances should any new licensing files be added to this directory
without careful consideration of how the project-wide LICENSE.txt and NOTICE.txt
files should be updated to reflect these additions. Even if a Jar being added is
from another Apache project, it should be mentioned in NOTICE.txt, and may have
additional Attribution or Licencing information that also needs to be added to
the appropriate file.

After adding or updating a dependency, the following steps should be taken to
store the correct dependency metadata.  (For directions for adding dependencies
to the build, see `help/dependencies.txt`.)

* Run `gradlew licenses` from the project root to print out which checksums,
   licenses, and notices are missing for each new dependency.
* Add the missing files.
    * Checksums can be generated automatically by running
      `gradlew updateLicenses` from the root directory.
    * License files must be found manually for each dependency.  The license
      should be listed on the project's website, or at the root level of its
      source tree.  Artifact repositories such as 'search.maven.org' often
      include licensing information and can be helpful here as well.
    * NOTICE files must also be found manually for each dependency, where they
      exist.  The terms of certain licenses (ASL, BSD, and CPL, primarily)
      require NOTICE files to be included.  For dependencies using these
      licenses, if a NOTICE does not exist a blank file can be created with the
      expected NOTICE name.  This allows our build process to validate the
      presence of these files without false-positives.
* Re-run `gradlew licenses` to ensure all per-dependency files are now in place.
* For each dependency, consider whether its licensing requires any changes to be
  made to Solr's root LICENSE and NOTICE.txt files.
  ?????? Is this still required, How would a developer know when a root
  ?????? NOTICE.txt change is merited?  An example change?  The entries in the
  ?????? root NOTICE.txt currently seem very spotty/arbitrary.
* Create or edit the CHANGES.txt entry for your change, ensuring that the new
  dependencies and versions are included in the message.  If the version is a
  "snapshot" of another project, include a specific git/svn revision number.

These steps should all be followed on dependency upgrades, as well as new
additions.  License and notice information often changes across versions of the
same dependency, so checking each time is always required.
