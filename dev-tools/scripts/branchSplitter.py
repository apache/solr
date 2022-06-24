#!/usr/bin/env python3

import glob
import subprocess
import sys

old_dir = "../solr2/"
new_dir = "./"
old_file_names = glob.glob(old_dir + "**/*.java", recursive=True)

changed_file_name_stems = []
candidate_file_name_stems = []

for old_file_name in old_file_names:
    file_name_stem = old_file_name[len(old_dir):]

    with open(old_dir + file_name_stem, 'r') as old_file, \
         open(new_dir + file_name_stem, 'r') as new_file:
        old_lines = old_file.readlines()
        new_lines = new_file.readlines()

        if len(old_lines) != len(new_lines):
            changed_file_name_stems.append(file_name_stem)
            continue

        sawChangedLine = False
        sawOnlySimpleChanges = True

        for idx in range(0, len(old_lines)):

            if old_lines[idx] == new_lines[idx]:
                continue
            else:
                sawChangedLine = True

            # identify start of line(s) that matches
            ss = 0
            while ss < len(old_lines[idx]) and ss < len(new_lines[idx]) and old_lines[idx][ss] == new_lines[idx][ss]:
              ss += 1

            # identify end of line(s) that matches
            ee = -1
            while -ee <= len(old_lines[idx]) and -ee <= len(new_lines[idx]) and old_lines[idx][ee] == new_lines[idx][ee]:
              ee -= 1

            # does difference look like template arguments?
            if (old_lines[idx][ss-1] == '<' and new_lines[idx][ss-1] == '<') and \
               (old_lines[idx][ee+1] == '>' and new_lines[idx][ee+1] == '>'):
                print(old_lines[idx].replace("\n", ""))
                print(new_lines[idx].replace("\n", ""))
            else:
                sawOnlySimpleChanges = False

        if sawChangedLine:
            changed_file_name_stems.append(file_name_stem)

        if sawChangedLine and sawOnlySimpleChanges:
            candidate_file_name_stems.append(file_name_stem)


print("# Counted " + str(len(changed_file_name_stems)) + " changed files.")
print("# Suggesting " + str(len(candidate_file_name_stems)) + " files be split into a separate branch.")

if 1 < len(sys.argv) and "" != sys.argv[1]:
    base_commit = sys.argv[1]

    for file_name_stem in changed_file_name_stems:
        if file_name_stem not in candidate_file_name_stems:
            subprocess.run(["git", "checkout", base_commit, "--", file_name_stem])

