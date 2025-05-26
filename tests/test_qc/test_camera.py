"""Tests for the QC camera module."""

from unittest.mock import Mock

import cv2
import numpy as np
import pandas as pd
import pytest

from contraqctor.qc.base import Status
from contraqctor.qc.camera import CameraTestSuite


class MockCameraData:
    """Mock CameraData class for testing."""

    def __init__(self, metadata=None, video_path=None, video_frame_count=100, has_video=True):
        self.metadata = metadata if metadata is not None else pd.DataFrame()
        self.video_path = video_path
        self._video_frame_count = video_frame_count
        self._has_video = has_video

    @property
    def has_video(self):
        return self._has_video

    @property
    def video_frame_count(self):
        return self._video_frame_count

    def as_video_capture(self):
        class MockContextManager:
            def __init__(self, mock_video):
                self.mock_video = mock_video

            def __enter__(self):
                return self.mock_video

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        mock_video = Mock()
        mock_video.get.side_effect = lambda prop: {
            cv2.CAP_PROP_FRAME_COUNT: self._video_frame_count,
            cv2.CAP_PROP_POS_FRAMES: 0,
        }.get(prop, 0)
        mock_video.read.return_value = (True, np.zeros((100, 100, 3), dtype=np.uint8))
        return MockContextManager(mock_video)


class MockCamera:
    """Mock Camera class for testing."""

    def __init__(self, data=None, has_data=True):
        self._data = data
        self._has_data = has_data

    @property
    def has_data(self):
        return self._has_data

    @property
    def data(self):
        if not self._has_data:
            raise ValueError("Camera data stream has no data")
        return self._data


@pytest.fixture
def valid_metadata():
    """Fixture providing valid metadata."""
    times = np.arange(0.0, 1.0, 1.0 / 30)
    return pd.DataFrame(
        {
            "CameraFrameNumber": range(30),
            "CameraFrameTime": times * 1e9,  # Convert to nanoseconds
        },
        index=pd.Index(times, name="ReferenceTime"),
    )


@pytest.fixture
def dropped_frames_metadata():
    """Fixture providing metadata with dropped frames."""
    times = np.linspace(0.0, 1.0, 30)
    frame_numbers = list(range(30))
    frame_numbers[15] = 20  # Skip frames 16-19
    return pd.DataFrame(
        {"CameraFrameNumber": frame_numbers, "CameraFrameTime": times * 1e9},
        index=pd.Index(times, name="ReferenceTime"),
    )


@pytest.fixture
def jittery_metadata():
    """Fixture providing metadata with clock jitter."""
    times = np.linspace(0.0, 1.0, 30)
    camera_times = np.array(times)
    camera_times[10] = times[10] + 0.01  # Add jitter to one frame
    return pd.DataFrame(
        {"CameraFrameNumber": range(30), "CameraFrameTime": camera_times * 1e9},
        index=pd.Index(times, name="ReferenceTime"),
    )


class TestCameraTestSuite:
    """Tests for the CameraTestSuite class."""

    def test_init(self, valid_metadata):
        """Test initializing the CameraTestSuite."""
        camera_data = MockCameraData(metadata=valid_metadata)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream, expected_fps=30)

        assert suite.data_stream == camera_stream
        assert suite.expected_fps == 30
        assert suite.clock_jitter_s == 1e-4
        assert suite.start_time_s is None
        assert suite.stop_time_s is None

    def test_metadata_shape(self, valid_metadata):
        """Test test_metadata_shape method."""
        # Test with valid metadata
        camera_data = MockCameraData(metadata=valid_metadata)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream)

        result = suite.test_metadata_shape()
        assert result.status == Status.PASSED
        assert "expected shape and columns" in result.message

        # Test with invalid metadata (missing column)
        invalid_metadata = valid_metadata.drop("CameraFrameNumber", axis=1)
        camera_data = MockCameraData(metadata=invalid_metadata)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream)

        result = suite.test_metadata_shape()
        assert result.status == Status.FAILED
        assert "Missing" in result.message

        # Test with empty metadata
        empty_metadata = pd.DataFrame()
        camera_data = MockCameraData(metadata=empty_metadata)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream)

        result = suite.test_metadata_shape()
        assert result.status == Status.FAILED

        # Test with no data
        camera_stream = MockCamera(has_data=False)
        suite = CameraTestSuite(camera_stream)
        result = suite.test_metadata_shape()
        assert result.status == Status.FAILED
        assert "does not have loaded data" in result.message

    def test_check_dropped_frames(self, valid_metadata, dropped_frames_metadata, jittery_metadata):
        """Test test_check_dropped_frames method."""
        # Test with valid metadata (no dropped frames)
        camera_data = MockCameraData(metadata=valid_metadata)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream)

        result = suite.test_check_dropped_frames()
        assert result.status == Status.PASSED
        assert "No dropped frames" in result.message

        # Test with dropped frames
        camera_data = MockCameraData(metadata=dropped_frames_metadata)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream)

        result = suite.test_check_dropped_frames()
        assert result.status == Status.FAILED
        assert "dropped frames" in result.message

        # Test with clock jitter
        camera_data = MockCameraData(metadata=jittery_metadata)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream, clock_jitter_s=0.001)  # Small threshold to detect jitter

        result = suite.test_check_dropped_frames()
        assert result.status == Status.FAILED
        assert "difference between CameraFrameTime and ReferenceTime" in result.message

    def test_match_expected_fps(self, valid_metadata):
        """Test test_match_expected_fps method."""
        # Test with matching FPS
        camera_data = MockCameraData(metadata=valid_metadata)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream, expected_fps=30)

        result = suite.test_match_expected_fps()
        assert result.status == Status.PASSED
        assert "within expected range" in result.message

        # Test with non-matching FPS
        suite = CameraTestSuite(camera_stream, expected_fps=60)
        result = suite.test_match_expected_fps()
        assert result.status == Status.FAILED
        assert "different than expected" in result.message

        # Test with no expected_fps
        suite = CameraTestSuite(camera_stream)
        result = suite.test_match_expected_fps()
        assert result.status == Status.SKIPPED
        assert "No expected FPS provided" in result.message

        # Test with high std deviation in frame period
        # Create metadata with variable frame rate
        times = np.concatenate([np.linspace(0, 0.5, 15), np.linspace(0.6, 1.2, 15)])
        variable_metadata = pd.DataFrame(
            {"CameraFrameNumber": range(30), "CameraFrameTime": times * 1e9},
            index=pd.Index(times, name="ReferenceTime"),
        )

        camera_data = MockCameraData(metadata=variable_metadata)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream, expected_fps=30)

        result = suite.test_match_expected_fps()
        assert result.status == Status.FAILED
        assert "High std in frame period" in result.message

    def test_is_start_bounded(self, valid_metadata):
        """Test test_is_start_bounded method."""
        camera_data = MockCameraData(metadata=valid_metadata)
        camera_stream = MockCamera(data=camera_data)

        # Test without start time
        suite = CameraTestSuite(camera_stream)
        result = suite.test_is_start_bounded()
        assert result.status == Status.SKIPPED
        assert "No start time provided" in result.message

        # Test with valid start time (before first frame)
        suite = CameraTestSuite(camera_stream, start_time_s=-0.1)
        result = suite.test_is_start_bounded()
        assert result.status == Status.PASSED
        assert "Start time is bounded" in result.message

        # Test with invalid start time (after first frame)
        suite = CameraTestSuite(camera_stream, start_time_s=0.1)
        result = suite.test_is_start_bounded()
        assert result.status == Status.FAILED
        assert "Start time is not bounded" in result.message

    def test_is_stop_bounded(self, valid_metadata):
        """Test test_is_stop_bounded method."""
        camera_data = MockCameraData(metadata=valid_metadata)
        camera_stream = MockCamera(data=camera_data)

        # Test without stop time
        suite = CameraTestSuite(camera_stream)
        result = suite.test_is_stop_bounded()
        assert result.status == Status.SKIPPED
        assert "No stop time provided" in result.message

        # Test with valid stop time (after last frame)
        suite = CameraTestSuite(camera_stream, stop_time_s=1.5)
        result = suite.test_is_stop_bounded()
        assert result.status == Status.PASSED
        assert "Stop time is bounded" in result.message

        # Test with invalid stop time (before last frame)
        suite = CameraTestSuite(camera_stream, stop_time_s=0.5)
        result = suite.test_is_stop_bounded()
        assert result.status == Status.FAILED
        assert "Stop time is not bounded" in result.message

    def test_video_frame_count(self, valid_metadata):
        """Test test_video_frame_count method."""
        # Test with matching frame counts
        camera_data = MockCameraData(metadata=valid_metadata, video_frame_count=30)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream)

        result = suite.test_video_frame_count()
        assert result.status == Status.PASSED
        assert "matches number of rows" in result.message

        # Test with mismatched frame counts
        camera_data = MockCameraData(metadata=valid_metadata, video_frame_count=40)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream)

        result = suite.test_video_frame_count()
        assert result.status == Status.FAILED
        assert "does not match number of rows" in result.message

        # Test with no video
        camera_data = MockCameraData(metadata=valid_metadata, has_video=False)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream)

        result = suite.test_video_frame_count()
        assert result.status == Status.SKIPPED
        assert "No video data available" in result.message

    def test_histogram_and_create_asset(self, valid_metadata):
        """Test test_histogram_and_create_asset method."""
        # Test with valid video
        camera_data = MockCameraData(metadata=valid_metadata)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream)

        result = suite.test_histogram_and_create_asset()
        assert result.status == Status.PASSED
        assert "Histogram and asset created successfully" in result.message
        assert "context" in dir(result)

        # Test with no video
        camera_data = MockCameraData(metadata=valid_metadata, has_video=False)
        camera_stream = MockCamera(data=camera_data)
        suite = CameraTestSuite(camera_stream)

        result = suite.test_histogram_and_create_asset()
        assert result.status == Status.SKIPPED
        assert "No video data available" in result.message
