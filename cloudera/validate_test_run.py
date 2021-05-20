#!/usr/bin/env python
import glob
import os
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict


class SurefireUtils:
    """
    Let's set the stage by stating the following:

    1. We use scalatest to write the scala unit tests and JUnit for the java unit tests.
    2. scalatest and maven-surefire-plugin generates the XML reports/files on a Suite basis.
    These XML reports contain relevant information about each test executed (conditions apply in
    case of scalatest).
    3. surefire-reports plugin uses these generated XMLs to aggregate and create a single
    surefire-report.html
    4. scalatest-maven-plugin is used to execute scala tests and maven-surefire-plugin is used to
    execute JUnit tests.

    ============================================================================================

    If a JUnit test causes an abort, it reports it as a test error and records this error in the
    XML. In order to validate this, run the following test and check "target/surefire-reports",
    there will be a "TEST-test.org.apache.spark.JavaAPISuite.xml".
    ```
        // Add the following test case in JavaAPISuite
        @Test
        public void dummy() {
            throw new LinkageError("junit is superior");
        }
    }
    ```
    This failure is categorized as an Error state for JUnit and the corresponding count in the
    following summary:
    ```
    [ERROR] Errors:
    [ERROR]   JavaAPISuite.dummy:103 Linkage junit is superior
    [INFO]
    [ERROR] Tests run: 1, Failures: 0, Errors: 1, Skipped: 0
    [INFO]
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD FAILURE
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 02:04 min
    [INFO] Finished at: 2021-05-20T18:15:03-07:00
    [INFO] ------------------------------------------------------------------------
    ```

    Thus an abort in java tests will be recorded in the surefire-reports xml and we can identify it.

    ============================================================================================

    If a ScalaTest causes an abort (e.g. FlatBuffers issue (CDPD-25958), NoSuchMethodError which
    inherits LinkageError causes JVM to abort) then scalatest exits the JVM without reporting it
    as a failure. In order to validate this, run the following test and check
    "target/surefire-reports", there won't be a "Test-<package>.Dummy.xml".
    ```
    class Dummy extends SparkFunSuite {
        test("is gonna fail") {
            throw new LinkageError("junit is superior")
        }
    }
    ```
    This is because the scalatest/scalatest-maven-plugin framework does not have an Error state,
    it just has the following states: [Succeeded, Failed, Canceled, Ignored, Pending]. If a scala
    test causes an exception that should lead to an abort, it does not classify it as a failure,
    it just exits with a non-zero exit code. It relies on the fact that the scalatest-maven-plugin
    will use this exit code to fail the maven build. However, when we set
    "maven.test.failure.ignore=true", scalatest-maven-plugin ignores the exit code from scalatest
    and continues. The XmlReporter responsible for generating the XML files, buffers all the
    events, and writes those to the XML file at the end of a Suite. But since the RUN is aborted
    abruptly, the scalatest code does not reach the point where it can ask XmlReporter to dump
    all the buffered events to a file.
    https://github.com/scalatest/scalatest/blob/a64b88a0c9576c5ffe19b673a1b3cfa48730f06b/scalatest/src/main/scala/org/scalatest/tools/XmlReporter.scala#L56

    However, this "*** RUN ABORTED ***" is recorded in a text file i.e.
    "target/surefire-reports/SparkTestSuite.txt", this is because the
    "<filereports>SparkTestSuite.txt</filereports>" reporter that we use flushes each event to a
    text file. (FileReporter inherits PrintReporter)
    https://github.com/scalatest/scalatest/blob/a64b88a0c9576c5ffe19b673a1b3cfa48730f06b/scalatest/src/main/scala/org/scalatest/tools/PrintReporter.scala#L148


    Thus an abort in scala tests won't be recorded in the surefire-reports xml. So we will have to
    fish for "RUN ABORTED" in "target/surefire-reports/SparkTestSuite.txt" to get the scala test
    error count.

    """
    TEST_XML_PREFIX = "TEST-*.xml"
    ERROR_ATTR = "errors"
    FAILURE_ATTR = "failures"

    def __init__(self, surefire_report_dirs, surefire_report_txt_file):
        assert len(surefire_report_dirs) != 0, "surefire_report_dirs cannot be empty"
        assert len(surefire_report_txt_file) != 0, "surefire_report_txt_file cannot be empty"

        self.metrics = defaultdict(int)
        self._parse_xml_test_reports(surefire_report_dirs)
        self._parse_txt_test_reports(surefire_report_dirs, surefire_report_txt_file)

    def _parse_xml_test_reports(self, surefire_report_dirs):
        for report_dir in surefire_report_dirs.split(os.linesep):
            xml_report_files = glob.iglob("{}/{}".format(report_dir, self.TEST_XML_PREFIX))
            for xml_report_file in xml_report_files:
                self._parse_xml_report(xml_report_file)

    def _parse_xml_report(self, xml_report_file):
        report_xml = ET.parse(xml_report_file)
        root = report_xml.getroot()
        assert root.tag == "testsuite", \
            "root tag is not 'testsuite' for {xml_report_file}, please update the parsing code".format(
                xml_report_file=xml_report_file)

        for attr in [self.ERROR_ATTR, self.FAILURE_ATTR]:
            value = root.attrib.get(attr)
            assert value is not None, \
                "cannot find '{attr}' attribute in <testsuite> for {xml_report_file}".format(
                    attr=attr, xml_report_file=xml_report_file)
            count = int(value)
            self.metrics[attr] += count
            if count > 0:
                print("{xml_report_file} has {count} {attr}".format(xml_report_file=xml_report_file,
                                                                    count=count, attr=attr))

    def _parse_txt_test_reports(self, surefire_report_dirs, surefire_report_txt_file):
        run_aborted_str = "RUN ABORTED"
        for report_dir in surefire_report_dirs.split(os.linesep):
            report_file_path = "{}/{}".format(report_dir, surefire_report_txt_file)
            if os.path.isfile(report_file_path):
                with open(report_file_path, 'r') as f:
                    lines = f.readlines()
                    count = sum([1 for line in lines if run_aborted_str in line])
                    if count:
                        print("{report_file_path} has {count} {run_aborted_str}".format(
                            report_file_path=report_file_path, run_aborted_str=run_aborted_str,
                            count=count))
                        # printing the last 15 lines to get a look at the stack trace.
                        print("".join(lines[-15:]))
                        self.metrics[self.ERROR_ATTR] += count

    def get_test_failure_count(self):
        return self.metrics[self.FAILURE_ATTR]

    def get_test_error_count(self):
        return self.metrics[self.ERROR_ATTR]


def main(surefire_report_dirs, surefire_txt_reports):
    utils = SurefireUtils(surefire_report_dirs, surefire_txt_reports)
    test_failure_count = utils.get_test_failure_count()
    test_error_count = utils.get_test_error_count()
    print("Test failures: {}".format(test_failure_count))
    print("Test errors: {}".format(test_error_count))
    assert test_failure_count == 0, "Test failures: {}".format(test_failure_count)
    assert test_error_count == 0, "Test errors: {}".format(test_error_count)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
