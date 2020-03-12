import filecmp
import pytest
import os
import shutil
import subprocess
import sys

if (sys.version_info > (3, 0)):
  from unittest.mock import MagicMock
  from unittest.mock import patch
else:
  from mock import MagicMock
  from mock import patch

import cloudera_helpers

USED_ENV_VARS = set(["__TEST_CONF_FILE__", "SPARK_DEV_BQUERY", "SPARK_DEV_BUILD_GBN", 
  "SPARK_DEV_APPLY_GBN_PATCH", "SPARK_DEV_GBN_ISOLATED_BUILD", "SPARK_DEV_CDPD_BUILD"])
  
class ConfFile(object):
  def __init__(self, filename):
    self.outfile = "tests/output/%s" % filename
    shutil.copy("tests/input/%s" % filename, self.outfile)
    os.environ["__TEST_CONF_FILE__"] = os.path.abspath(self.outfile)

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    if os.path.exists(self.outfile):
      os.remove(self.outfile)
    del os.environ["__TEST_CONF_FILE__"]

_patch_dir = "tests/input/git_patches"
_all_patches = sorted([
  os.path.abspath("tests/input/git_patches/%s" % f) \
  for f in os.listdir(_patch_dir) \
  if f.endswith(".patch")
])
_regular_patches = [f for f in _all_patches if "000" in f]
_gbn_patches = [f for f in _all_patches if "__GBN_PATCH_" in f]

_output_git_dir = "tests/output/dummy_repo"

def _git_apply_patch(n):
  subprocess.check_call(["git", "-C", _output_git_dir, "am", _regular_patches[n]])

def _git_apply_gbn_patch(n):
  subprocess.check_call(["git", "-C", _output_git_dir, "am", _gbn_patches[n]])

def setup_function(f):
  global ORIG_ENV
  ORIG_ENV = {key: os.getenv(key) for key in USED_ENV_VARS}
  os.environ["SPARK_DEV_CDPD_BUILD"]="1"
  if os.path.exists("tests/output"):
    shutil.rmtree("tests/output")
  os.mkdir("tests/output")


def teardown_function(f):
  if os.path.exists("tests/output"):
    shutil.rmtree("tests/output")
  for key in ORIG_ENV:
    orig_value = ORIG_ENV.get(key)
    if orig_value:
      os.environ[key] = orig_value
    elif os.getenv(key):
      del os.environ[key]

def compare_files(exp, act):
  with open(exp, "r") as e_in, open(act, "r") as a_in:
    for e_line, a_line in zip(e_in, a_in):
       assert(e_line == a_line)


@patch('cloudera_helpers._get_gbn_for_bquery')
def test_resolve_bquery_when_necessary(mock_bquery_fn):
  mock_bquery_fn.return_value="12345"
  with ConfFile("nonexistent.conf") as conf:
    cloudera_helpers.main(conf.outfile)
    mock_bquery_fn.assert_called_with("cdh7.2.1")
    compare_files("tests/input/with_gbn.conf", conf.outfile)


@patch('cloudera_helpers._get_gbn_for_bquery')
def test_resolve_bquery_when_necessary(mock_bquery_fn):
  mock_bquery_fn.return_value="12345"
  with ConfFile("without_gbn.conf") as conf:
    cloudera_helpers.main(conf.outfile)
    mock_bquery_fn.assert_called_with("cdh7.2.1")
    compare_files("tests/input/with_gbn.conf", conf.outfile)


@patch('cloudera_helpers._get_gbn_for_bquery')
def test_skip_conf_generation_when_complete(mock_bquery_fn):
  with ConfFile("with_gbn.conf") as conf:
    # normally, the conf file is loaded at the bash-script level
    # so we do that part manually here.
    os.environ["SPARK_DEV_BUILD_GBN"]="12345"
    cloudera_helpers.main(conf.outfile)
    mock_bquery_fn.assert_not_called()
    compare_files("tests/input/with_gbn.conf", conf.outfile)


@patch('cloudera_helpers._download_gbn_patch')
def test_apply_simple_gbn_patch(mock_download_fn):
  subprocess.check_call(["git", "init", _output_git_dir])
  for idx in range(4):
    _git_apply_patch(idx)

  def mock_download(gbn, dir):
    shutil.copy(_gbn_patches[0], _output_git_dir + "/spark-source.patch")
  mock_download_fn.side_effect = mock_download
  cloudera_helpers.maybe_apply_gbn_patch(1, _output_git_dir)
  mock_download_fn.assert_called_once()
  log_output = \
    subprocess.check_output(["git", "log", "-1", "--format=%s"], cwd=_output_git_dir).splitlines()
  assert(log_output[0] == "__GBN_PATCH__1_")


@patch('cloudera_helpers._download_gbn_patch')
def test_revert_and_apply_gbn_patch(mock_download_fn):
  subprocess.check_call(["git", "init", _output_git_dir])
  for idx in range(4):
    _git_apply_patch(idx)
  _git_apply_gbn_patch(0)
  # first just check test setup -- we've created a git repo with a GBN PATCH
  assert("__GBN_PATCH__1_" in subprocess.check_output(["git", "log", "-1", "--format=%s"], cwd=_output_git_dir))

  # now for the real test -- call our function, make sure it does the right thing
  def mock_download(gbn, dir):
    shutil.copy(_gbn_patches[1], _output_git_dir + "/spark-source.patch")
  mock_download_fn.side_effect = mock_download
  cloudera_helpers.maybe_apply_gbn_patch(2, _output_git_dir)
  mock_download_fn.assert_called_once()
  log_output = \
    subprocess.check_output(["git", "log", "-2", "--format=%s"], cwd=_output_git_dir).splitlines()
  assert(log_output[0] == "__GBN_PATCH__2_")
  assert(log_output[1] == "Revert \"__GBN_PATCH__1_\"")


@patch('cloudera_helpers._download_gbn_patch')
def test_dont_revert_last_revert(mock_download_fn):
  subprocess.check_call(["git", "init", _output_git_dir])
  for idx in range(4):
    _git_apply_patch(idx)
  _git_apply_gbn_patch(0)
  subprocess.check_call(["git", "revert", "HEAD"], cwd=_output_git_dir)
  original_log = subprocess.check_output(["git", "log", "-2", "--format=%H %s"], cwd=_output_git_dir).splitlines()
  # check test setup
  assert("Revert \"__GBN_PATCH__1_\"" in original_log[0])
  assert("__GBN_PATCH__1_" in original_log[1])

  # now for the real test -- call our function, make sure it does the right thing
  def mock_download(gbn, dir):
    shutil.copy(_gbn_patches[1], _output_git_dir + "/spark-source.patch")
  mock_download_fn.side_effect = mock_download
  cloudera_helpers.maybe_apply_gbn_patch(2, _output_git_dir)
  mock_download_fn.assert_called_once()
  log_output = \
    subprocess.check_output(["git", "log", "-2", "--format=%H %s"], cwd=_output_git_dir).splitlines()
  # just one new commit here, no extra revert.
  assert("__GBN_PATCH__2_" in log_output[0])
  assert(log_output[1] == original_log[0])


@pytest.mark.xfail
@patch('cloudera_helpers._download_gbn_patch')
def test_fail_on_failed_revert(mock_download_fn):
  subprocess.check_call(["git", "init", _output_git_dir])
  for idx in range(4):
    _git_apply_patch(idx)
  _git_apply_gbn_patch(0)
  ## TODO figure out some commits for this ...
  pass


@patch('cloudera_helpers._download_gbn_patch')
def test_apply_gbn_patch_only_once(mock_download_fn):
  subprocess.check_call(["git", "init", _output_git_dir])
  for idx in range(4):
    _git_apply_patch(idx)
  _git_apply_gbn_patch(0)
  cloudera_helpers.maybe_apply_gbn_patch(1, _output_git_dir)
  mock_download_fn.assert_not_called()

@patch('cloudera_helpers._download_gbn_patch')
def test_fail_applying_gbn_if_tree_is_dirty(mock_download_fn):
  subprocess.check_call(["git", "init", _output_git_dir])
  _git_apply_patch(0)
  with open(_output_git_dir + "/pom.xml", "wa") as out:
    out.write("test")
  stdout = subprocess.check_output(["git", "status"], cwd=_output_git_dir)
  with pytest.raises(RuntimeError, match=r"Your git tree is dirty"):
    cloudera_helpers.maybe_apply_gbn_patch("12345", _output_git_dir)
  mock_download_fn.assert_not_called()

def test_do_nothing_without_env_var_set():
  # this will always be set by the setup function, as we want it for every
  # other test
  del os.environ["SPARK_DEV_CDPD_BUILD"]
  stdout = subprocess.check_output(["build/mvn", "help:evaluate", "-Dexpression=project.repositories", "-q", "-DforceStdout"], cwd="..")
  assert("Skipping dev GBN build logic" in stdout)
  # make sure the gbn repo is not included
  assert("<id>gbnRepo</id>" not in stdout)
  assert("s3/build/${env.SPARK_DEV_BUILD_GBN}" not in stdout)

def test_download_from_gbn_specific_repo():
  with ConfFile("with_gbn.conf") as conf:
    stdout = subprocess.check_output(["build/mvn", "help:evaluate", "-Dexpression=project.repositories", "-q", "-DforceStdout"], cwd="..")
    assert("executing cloudera_helpers.py" in stdout)
    # make sure the gbn repo gets expanded in mvn
    assert("s3/build/12345" in stdout)

def test_local_repo_mvn():
  with ConfFile("gbn_isolated_build.conf") as conf:
    stdout = subprocess.check_output(["../build/mvn", "help:evaluate", "-Dexpression=settings.localRepository", "-q", "-DforceStdout"])
    assert("gbnRepos/12345" in stdout)

# a test for the local repo w/ sbt involves re-downloading so much of sbt just to start
# thats its too slow to run regularly
@pytest.mark.skip
def test_local_repo_sbt():
  with ConfFile("gbn_isolated_build.conf") as conf:
    stdout = subprocess.check_output(["../build/sbt", "show ivyConfiguration"])
    assert("gbnRepos/12345_ivy" in stdout)

