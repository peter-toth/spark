#!/usr/bin/env python

# This is a helper for working with internal builds, giving a few
# options on how to control which CDPD build you use.  It
# a) updates a "build.conf" file, which can be easily sourced in
#    the build/mvn & build/sbt bash scripts to control behavior
# b) (optionally) applies a patch file, like the official builds
#
# Its written in python to make it easier to understand & test

import errno
import json
import os
import subprocess
import sys
import textwrap
import wget

DEFAULT_BQUERY="cdh7.2.1"

def _get_gbn_for_bquery(bquery):
  try:
    subprocess.check_call(["command", "-v", "buildinfo"])
  except Exception as cause:
    msg = textwrap.dedent("""\
      'buildinfo' is not installed, aborting.  You can try installing with

       pip install buildinfo \\
          --index-url https://pypi.infra.cloudera.com/api/pypi/cloudera/simple""")
    raise RuntimeError(msg + u', caused by ' + repr(cause)), None, sys.exc_info()[2]

  gbn = subprocess.check_output(["buildinfo", "resolve", bquery])
  print("resolved bquery %s into gbn %s" % (bquery, gbn))
  return gbn

def _download_gbn_patch(gbn, dir):
  url = "http://cloudera-build-us-west-1.vpc.cloudera.com/s3/build/%s/PATCH_FILES/centos7/patch_files/spark-source.patch" % gbn
  wget.download(url, dir + "/spark-source.patch")


def maybe_apply_gbn_patch(gbn, dir):
  """Apply the spark-source patch for the given GBN, with the following conditions:
      1. If the tree is dirty, fail
      2. If a patch was already applied for the same GBN, do nothing
      3. If a patch was already applied for another GBN, first revert that patch
  """
  gbn_msg = "__GBN_PATCH__%s_" % gbn
  # check if there is already a gbn patch.  This uses the convention that only
  # these automatic GBN patches will have with "__GBN_PATCH__{GBN}_"
  log_grep_output = subprocess.check_output(
    ["git", "log", "--grep", "^\(\(__GBN_PATCH__\)\|\(Revert \"__GBN_PATCH__\)\)", "-n", "1", "--format=%H %s"], cwd=dir)
  print("log_grep_output= %s" % log_grep_output)
  print(gbn_msg)
  if log_grep_output:
    if gbn_msg in log_grep_output:
      if "Revert" in log_grep_output:
        pass
      else:
        print("GBN PATCH for %s already applied" % gbn)
        return
    elif "Revert" not in log_grep_output:
      prior_sha = log_grep_output.split()[0]
      print("Reverting prior build patch '%s'" % log_grep_output)
      try:
        subprocess.check_call(["git", "revert", "--no-edit", prior_sha], cwd=dir)
      except:
        raise RuntimeError("Failed to revert prior build patch '%s'.  You can fix this " +
          "up manually and run 'git revert --continue', or abandon this revert with " +
          "'git revert --abort'"), None, sys.exc_info()[2]

  try:
    subprocess.check_call(["git", "diff", "--quiet"], cwd=dir)
  except:
    raise RuntimeError("Your git tree is dirty; refusing to apply gbn patch.  " +
      "Try stashing first."), None, sys.exc_info()[2]
  # now we can actually apply the patch for the GBN we want
  _download_gbn_patch(gbn, dir)
  try:
    subprocess.check_call(["git", "apply", "--whitespace=fix", "--reject", "spark-source.patch"], cwd=dir)
  except:
    raise RuntimeError(("Failed to apply patch for GBN %s cleanly.  Check .rej files.  " +
      "You can fix manually, the commit with msg '%s'.  You might just need to rebase " +
      "your changes.") % (gbn, gbn_msg)), None, sys.exc_info()[2]

  subprocess.check_call(["git", "add", "-u", "."], cwd=dir)
  subprocess.check_call(["git", "commit", "-m", gbn_msg], cwd=dir)


def save_conf_file(dest, conf):
  print("Saving build conf to %s" % dest)
  conf_string = """\
# Which cdpd-release you want to build against
export SPARK_DEV_BQUERY=%(bquery)s

# Which specific build you want to use for the rest of the cdpd-artifacts.  If you delete the line
# below, then the script will automatically find the last GBN for the release line.
export SPARK_DEV_BUILD_GBN=%(gbn)s

# If you're worried your local maven repo has a mix of old snapshot artifacts, this is a
# "nuclear option" -- it uses a gbn-specific local maven repo.  This means you'll need to
# redownload *everything*, so be prepared for a slow build.
export SPARK_DEV_GBN_ISOLATED_BUILD=%(gbn_isolated_build)s

# Alternatively, apply the GBN-specific patch file to your local repo and commit it.
# You don't want to check that change in when you finally commit, but is the most like
# the "real" build.
export SPARK_DEV_APPLY_GBN_PATCH=%(apply_gbn_patch)s
""" % conf
  with open(dest, "w") as out:
    out.write(conf_string)


def mkdir_p(path):
 try:
   os.makedirs(path)
 except OSError as exc:
   if exc.errno != errno.EEXIST and os.path.isdir(path):
     raise


def main(dest_conf_file):
  """Apply a patch if necessary, and create tmp conf file for the
     build scripts.

     Assumes the pre-existing conf file has already been loaded before
     this is executed.
  """
  conf = {}
  conf["bquery"] = os.getenv("SPARK_DEV_BQUERY") or DEFAULT_BQUERY
  conf["gbn"] = os.getenv("SPARK_DEV_BUILD_GBN") or _get_gbn_for_bquery(conf["bquery"])
  conf["apply_gbn_patch"] = os.getenv("SPARK_DEV_APPLY_GBN_PATCH") or "0"
  conf["gbn_isolated_build"] = os.getenv("SPARK_DEV_GBN_ISOLATED_BUILD") or "0"
  if conf["apply_gbn_patch"] == "1":
    maybe_apply_gbn_patch(conf["gbn"], dir=".")
  mkdir_p(os.path.dirname(dest_conf_file))
  save_conf_file(dest_conf_file, conf)


if __name__ == "__main__":
  main(sys.argv[1])
