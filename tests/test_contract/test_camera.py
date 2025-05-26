import os
from unittest.mock import patch

import pandas as pd
import pytest

from contraqctor.contract.camera import (
    CAP_PROP_FRAME_COUNT,
    CAP_PROP_FRAME_HEIGHT,
    CAP_PROP_FRAME_WIDTH,
    Camera,
    CameraData,
    CameraParams,
)


@pytest.fixture
def mock_camera_dir(temp_dir):
    """Fixture providing a directory with camera metadata and video files."""
    metadata_path = temp_dir / "metadata.csv"
    metadata = pd.DataFrame(
        {
            "ReferenceTime": [0.0, 0.1, 0.2, 0.3],
            "CameraFrameNumber": [0, 1, 2, 3],
            "CameraFrameTime": [1000.0, 1033.3, 1066.7, 1100.0],
        }
    )
    metadata.to_csv(metadata_path, index=False)

    video_path = temp_dir / "video.avi"
    with open(video_path, "wb") as f:
        # Write minimal valid AVI header (not a real video, just enough to be opened by VideoCapture)
        f.write(b"RIFF\x24\x00\x00\x00AVI \x10\x00\x00\x00LIST\x14\x00\x00\x00hdrlavih\x08\x00\x00\x00\x2a\x00\x00\x00")

    return temp_dir


class TestCamera:
    """Tests for the Camera class."""

    def test_read_camera(self, mock_camera_dir):
        """Test reading camera data."""
        camera_stream = Camera(name="test", reader_params=CameraParams(path=mock_camera_dir))

        with patch("contraqctor.contract.camera.VideoCapture") as mock_video:
            # Mock video properties
            mock_instance = mock_video.return_value
            mock_instance.get.side_effect = lambda prop: {
                CAP_PROP_FRAME_WIDTH: 640,
                CAP_PROP_FRAME_HEIGHT: 480,
                CAP_PROP_FRAME_COUNT: 30,
            }.get(prop, 0)

            data = camera_stream.read()

            assert isinstance(data, CameraData)
            assert isinstance(data.metadata, pd.DataFrame)
            assert len(data.metadata) == 4
            assert list(data.metadata.index) == [0.0, 0.1, 0.2, 0.3]
            assert list(data.metadata["CameraFrameNumber"]) == [0, 1, 2, 3]
            assert os.path.samefile(data.video_path, mock_camera_dir / "video.avi")

    def test_load_camera(self, mock_camera_dir):
        """Test loading camera data."""
        camera_stream = Camera(name="test", reader_params=CameraParams(path=mock_camera_dir))

        with patch("contraqctor.contract.camera.VideoCapture"):
            camera_stream.load()

            assert camera_stream.has_data
            assert isinstance(camera_stream.data, CameraData)
            assert isinstance(camera_stream.data.metadata, pd.DataFrame)
            assert len(camera_stream.data.metadata) == 4

    def test_missing_metadata_columns(self, temp_dir):
        """Test error when metadata is missing required columns."""
        metadata_path = temp_dir / "metadata.csv"
        metadata = pd.DataFrame(
            {
                "ReferenceTime": [0.0, 0.1],
                # Missing CameraFrameNumber
                "CameraFrameTime": [1000.0, 1033.3],
            }
        )
        metadata.to_csv(metadata_path, index=False)

        # Create a dummy video file
        video_path = temp_dir / "video.avi"
        with open(video_path, "wb") as f:
            f.write(b"dummy")

        camera_stream = Camera(name="test", reader_params=CameraParams(path=temp_dir))

        with pytest.raises(ValueError) as exc_info:
            camera_stream.read()

        assert "missing required columns" in str(exc_info.value)

    def test_missing_video(self, temp_dir):
        """Test error when video file is missing."""
        metadata_path = temp_dir / "metadata.csv"
        metadata = pd.DataFrame(
            {"ReferenceTime": [0.0, 0.1], "CameraFrameNumber": [0, 1], "CameraFrameTime": [1000.0, 1033.3]}
        )
        metadata.to_csv(metadata_path, index=False)

        camera_stream = Camera(name="test", reader_params=CameraParams(path=temp_dir))

        with pytest.raises(FileNotFoundError) as exc_info:
            camera_stream.read()

        assert "No video file found" in str(exc_info.value)


class TestCameraData:
    """Tests for the CameraData class."""

    def test_video_frame_count(self, mock_camera_dir):
        """Test getting video frame count."""
        metadata = pd.DataFrame(
            {
                "ReferenceTime": [0.0, 0.1, 0.2],
                "CameraFrameNumber": [0, 1, 2],
                "CameraFrameTime": [1000.0, 1033.3, 1066.7],
            }
        )
        metadata = metadata.set_index("ReferenceTime")

        data = CameraData(metadata=metadata, video_path=mock_camera_dir / "video.avi")

        with patch("contraqctor.contract.camera.VideoCapture") as mock_video:
            mock_instance = mock_video.return_value
            mock_instance.get.side_effect = lambda prop: 30 if prop == CAP_PROP_FRAME_COUNT else 0

            assert data.video_frame_count == 30

    def test_video_frame_size(self, mock_camera_dir):
        """Test getting video frame size."""
        metadata = pd.DataFrame(
            {
                "ReferenceTime": [0.0, 0.1, 0.2],
                "CameraFrameNumber": [0, 1, 2],
                "CameraFrameTime": [1000.0, 1033.3, 1066.7],
            }
        )
        metadata = metadata.set_index("ReferenceTime")

        data = CameraData(metadata=metadata, video_path=mock_camera_dir / "video.avi")

        with patch("contraqctor.contract.camera.VideoCapture") as mock_video:
            mock_instance = mock_video.return_value
            mock_instance.get.side_effect = lambda prop: {
                CAP_PROP_FRAME_WIDTH: 640,
                CAP_PROP_FRAME_HEIGHT: 480,
            }.get(prop, 0)

            assert data.video_frame_size == (640, 480)
