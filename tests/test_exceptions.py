import pytest

from jobrunner.exceptions import OpenSafelyError, RepoNotFound


class TestError(OpenSafelyError):
    status_code = 10


def test_exception_reporting():
    error = TestError("thing not to leak", report_args=False)
    assert error.safe_details() == "TestError: [possibly-unsafe details redacted]"
    assert repr(error) == "TestError('thing not to leak')"

    error = TestError("thing OK to leak", report_args=True)
    assert error.safe_details() == "TestError: thing OK to leak"
    assert repr(error) == "TestError('thing OK to leak')"


def test_reserved_exception():
    class InvalidError(OpenSafelyError):
        status_code = -1

    with pytest.raises(AssertionError) as e:
        raise InvalidError(report_args=True)
    assert "reserved" in e.value.args[0]

    with pytest.raises(RepoNotFound):
        raise RepoNotFound(report_args=True)
