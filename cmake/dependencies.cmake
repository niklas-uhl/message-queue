include(FetchContent)

FetchContent_Declare(
  kamping
  GIT_REPOSITORY https://github.com/kamping-site/kamping.git
  GIT_TAG v0.1.2
  SYSTEM
)

FetchContent_Declare(
  kassert
  GIT_REPOSITORY https://github.com/kamping-site/kassert.git
  GIT_TAG f0873f8
  SYSTEM
)

FetchContent_Declare(
  range-v3
  URL https://github.com/ericniebler/range-v3/archive/0.12.0.zip
  SYSTEM
  SOURCE_SUBDIR NON_EXISTANT
)

FetchContent_Declare(
  CLI11
  GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
  GIT_TAG v2.5.0
  SYSTEM
)
