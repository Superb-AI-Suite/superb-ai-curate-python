# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added `slice` parameter for `Dataset.add_images()` and `Image.create_bulk()` so that users can add newly added dataset images directly to a slice (sc-3564)
- Added annotation bulk delete jobs (sc-3570)

---

## [1.4.1.post1] - 2024-08-20

### Fixed

- Fixed readthedocs build

## [1.4.1] - 2024-08-19

### Fixed

- Fixed depedency resolution for `aiohttp` to support Python 3.12

## [1.4.0] - 2024-08-13

### Added

- Added upload_image_directory function for Dataset class (sc-3367)
- Added INFO level logs to track the local image upload progress (sc-3368)

### Changed

- Upload local images asynchronously (sc-3366)
- Retry local image upload on failure (sc-3366)

## [1.3.1] - 2024-05-27

### Fixed

- Fixed an issue where iou_type for creating a diagnosis was not being passed properly

## [1.3.0] - 2024-05-24

### Added

- Added train/val split to the prediction upload (sc-3156)
- Diagnosis metadata docstring clarification (sc-3156)

### Updated

- Documentation URL in pyproject

## [1.2.2] - 2024-04-11

### Fixed

- Fixed outdated parameters for update slice and update slice by query

## [1.2.1] - 2024-03-19

### Fixed

- Parsing logic for non-specified http response codes
- Override default retry logic to include POST requests

## [1.2.0] - 2024-02-16

### Added

- Annotation pagination with filtering

## [1.1.1.post1] - 2023-12-05

### Fixed

- Updated package version

## [1.1.1] - 2023-12-05

### Added

- Added image URL to images

## [1.1.0] - 2023-10-31

### Added

- Model Diagnosis support (sc-2473)

## [1.0.2] - 2023-10-12

### Fixed

- Fixed a duplicate issue for bulk create of local images

## [1.0.1] - 2023-08-25

### Fixed

- Corrected the Image search endpoint (sc-2141)

## [1.0.0] - 2023-08-22

### Added

- Added `Read the Docs` links to the README

### Changed

- Changed batch API parameter structure (sc-2332)

### Fixed

- Adjusted the vscode formatter settings
- Security update for requests to `>=2.31.0,<2.32.0`

## [1.0.0b1.post2] - 2023-05-31

### Changed

- Fix the Readthedocs build (sc-1178)

## [1.0.0b1.post1] - 2023-05-04

### Added

- Setup Coveralls badge in readme

### Changed

- Moved sphinx deployment from Github pages to Readthedocs (sc-1178)

## [1.0.0b1] - 2023-04-19

Initial beta release of the Superb AI Curate Python client.
