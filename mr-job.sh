#!/bin/bash

SCRIPT_NAME=$(basename "$0")
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

MR_TCP_PORT=${MR_TCP_PORT:-1234}
MR_NUM_WORKERS=${MR_NUM_WORKERS:-2}
MR_WAIT_TIME=${MR_WAIT_TIME:-5}

GO_IMAGE=${GO_IMAGE:-golang:tip-trixie}
RUNTIME_IMAGE=${RUNTIME_IMAGE:-debian:trixie-slim}

if [[ "$MR_NUM_WORKERS" -lt 1 ]]; then
    echo "MR_NUM_WORKERS=$MR_NUM_WORKERS cannot be less than 1, setting it to 1"
    MR_NUM_WORKERS=1
fi


# ============================================================
# Logging
# ============================================================

log() {
    if [[ "$#" != "2" ]]; then
        echo "ERROR: log function not used properly!"
        echo "Usage: log <level> <msg>"
        exit 1
    fi

    local log_level="$1"
    local log_msg="$2"

    echo "$log_level: $log_msg"
}

log_debug() {
    if [[ "${DEBUG:-0}" -eq 1 ]]; then
        log "DEBUG" "$@"
    fi
}

log_info() {
    log "INFO" "$@"
}

log_error() {
    log "ERROR" "$@"
}


# ============================================================
# Usage
# ============================================================

usage() {
    log_info "Usage: $SCRIPT_NAME [-h] <mr.go> <file-1> <file-2> ..."
    log_info ""
    log_info "  <mr.go>       MapReduce application source file"
    log_info "  <file-N>      Input files"
    log_info ""
    log_info "Environment variables:"
    log_info "  MR_TCP_PORT    [default: 1234]"
    log_info "  MR_NUM_WORKERS [default: 2]"
    log_info "  MR_WAIT_TIME   [default: 5]"
    log_info "  GO_IMAGE       [default: golang:tip-trixie]"
    log_info "  RUNTIME_IMAGE  [default: debian:trixie-slim]"
}


# ============================================================
# Arguments
# ============================================================

if [[ $# -eq 0 ]]; then
    log_error "Not Enough Arguments!"
    usage
    exit 1
fi

if [[ "$1" == "-h" ]]; then
    usage
    exit 0
fi

if [[ $# -lt 2 ]]; then
    log_error "Not Enough Arguments!"
    usage
    exit 1
fi


# ============================================================
# Resolve paths
# ============================================================

MR_SRC_DIR="$SCRIPT_DIR/src"

GO_FILE="$1"
shift

HOST_FILES=("$@")


GO_FILE_ABS=$(realpath "$GO_FILE" 2>/dev/null)

if [[ -z "$GO_FILE_ABS" || ! -f "$GO_FILE_ABS" ]]; then
    log_error "MapReduce source file does not exist:"
    log_error "  $GO_FILE"
    exit 1
fi


for i in "${!HOST_FILES[@]}"; do

    HOST_FILES[$i]=$(realpath "${HOST_FILES[$i]}" 2>/dev/null)

    if [[ -z "${HOST_FILES[$i]}" || ! -f "${HOST_FILES[$i]}" ]]; then
        log_error "Input file does not exist:"
        log_error "  ${HOST_FILES[$i]}"
        exit 1
    fi

done


# ============================================================
# Ensure Go source is inside src/
# ============================================================

if [[ "$GO_FILE_ABS" != "$MR_SRC_DIR/"* ]]; then
    log_error "MapReduce source file must be inside:"
    log_error "  $MR_SRC_DIR"
    log_error "Received:"
    log_error "  $GO_FILE"
    exit 1
fi


GO_FILE_RELATIVE="${GO_FILE_ABS#"$MR_SRC_DIR"/}"
GO_FILENAME=$(basename "$GO_FILE_ABS")

if [[ "$GO_FILENAME" != *.go ]]; then
    log_error "MapReduce source file must have a .go extension"
    exit 1
fi

SO_FILENAME="${GO_FILENAME%.go}.so"


# ============================================================
# Job-specific names
# ============================================================

JOB_TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
JOB_UNIQUE_ID="$JOB_TIMESTAMP-$$"

DOCKER_IMG="mr-job:$JOB_UNIQUE_ID"

SHARED_MNT_DIR="/mnt/temp"

SHARED_NET_NAME="tmp-net-$JOB_UNIQUE_ID"
COORDINATOR_NAME="mr-coordinator-$JOB_UNIQUE_ID"


log_info "MapReduce source:"
log_info "  $GO_FILE_ABS"

log_info "MapReduce plugin:"
log_info "  $SO_FILENAME"

log_info "Input files:"

for file in "${HOST_FILES[@]}"; do
    log_info "  $file"
done


# ============================================================
# Temporary directory
# ============================================================

TEMP_DIR=$(mktemp -d)

BUILD_CONTEXT="$TEMP_DIR/build"
INPUT_DIR="$TEMP_DIR/inputs"
OUTPUT_DIR="$TEMP_DIR/outputs"

mkdir -p "$TEMP_DIR/tmp"
mkdir -p "$BUILD_CONTEXT"
mkdir -p "$INPUT_DIR"
mkdir -p "$OUTPUT_DIR"

log_info "Temporary directory:"
log_info "  $TEMP_DIR"


# ============================================================
# Runtime state
# ============================================================

ALL_CONTAINER_IDS=()
LOG_PIDS=()


# ============================================================
# Cleanup
# ============================================================

cleanup() {

    log_info "Cleaning up..."

    for pid in "${LOG_PIDS[@]}"; do
        if [[ -n "$pid" ]]; then
            kill "$pid" 2>/dev/null || true
        fi
    done


    for container in "${ALL_CONTAINER_IDS[@]}"; do
        if [[ -n "$container" ]]; then
            log_info "Removing container: $container"
            docker rm -f -v "$container" 2>/dev/null || true
        fi
    done


    if docker network inspect "$SHARED_NET_NAME" >/dev/null 2>&1; then
        log_info "Removing network: $SHARED_NET_NAME"
        docker network rm "$SHARED_NET_NAME" 2>/dev/null || true
    fi


    if docker image inspect "$DOCKER_IMG" >/dev/null 2>&1; then
        log_info "Removing job image: $DOCKER_IMG"
        docker image rm "$DOCKER_IMG" >/dev/null 2>&1 || true
    fi


    # Keep the job directory for inspection.
    if [[ -d "$TEMP_DIR" ]]; then

        PRESERVED_DIR="$PWD/mr-job-tmp-$JOB_TIMESTAMP"

        log_info "Preserving temporary directory..."
        log_info "  From: $TEMP_DIR"
        log_info "  To:   $PRESERVED_DIR"

        if mv "$TEMP_DIR" "$PRESERVED_DIR"; then
            log_info "Temporary files preserved at:"
            log_info "  $PRESERVED_DIR"
        else
            log_error "Failed to preserve temporary directory."
            log_error "Temporary files remain at:"
            log_error "  $TEMP_DIR"
        fi
    fi
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ============================================================
# Prepare build context
# ============================================================

log_info "Preparing Docker build context..."

cp -a "$MR_SRC_DIR/." "$BUILD_CONTEXT/"


if [[ ! -f "$BUILD_CONTEXT/go.mod" ]]; then
    log_error "go.mod not found in $MR_SRC_DIR"
    exit 1
fi

if [[ ! -f "$BUILD_CONTEXT/go.sum" ]]; then
    log_error "go.sum not found in $MR_SRC_DIR"
    exit 1
fi


# ============================================================
# Copy input files
# ============================================================

log_info "Copying HOST_FILES to $INPUT_DIR"

if ! cp --parents "${HOST_FILES[@]}" "$INPUT_DIR"; then
    log_error "Failed to copy input files"
    exit 1
fi


# ============================================================
# Generate job-specific Dockerfile
# ============================================================

DOCKERFILE="$TEMP_DIR/Dockerfile"

cat > "$DOCKERFILE" <<EOF
FROM $GO_IMAGE AS builder

WORKDIR /usr/src/app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build \\
    CGO_ENABLED=1 go build \\
        -race \\
        -o /usr/bin/mrcoordinator \\
        main/mrcoordinator.go

RUN --mount=type=cache,target=/root/.cache/go-build \\
    CGO_ENABLED=1 go build \\
        -race \\
        -o /usr/bin/mrworker \\
        main/mrworker.go

RUN --mount=type=cache,target=/root/.cache/go-build \\
    CGO_ENABLED=1 go build \\
        -race \\
        -buildmode=plugin \\
        -o /usr/bin/$SO_FILENAME \\
        ./$GO_FILE_RELATIVE


FROM $RUNTIME_IMAGE

WORKDIR /mnt/temp

COPY --from=builder /usr/bin/mrcoordinator /usr/bin/mrcoordinator
COPY --from=builder /usr/bin/mrworker /usr/bin/mrworker
COPY --from=builder /usr/bin/$SO_FILENAME /usr/bin/$SO_FILENAME
EOF


# ============================================================
# Build job image
# ============================================================

log_info "Building Docker image: $DOCKER_IMG"

if ! docker build \
    --tag "$DOCKER_IMG" \
    --file "$DOCKERFILE" \
    "$BUILD_CONTEXT"; then

    log_error "Failed to build job image"
    exit 1
fi

log_info "Successfully built job image: $DOCKER_IMG"

DOCKER_USER="$(id -u):$(id -g)"

# ============================================================
# Verify image
# ============================================================

log_info "Verifying job image..."

docker run --rm "$DOCKER_IMG" /bin/bash -c "
    echo '--- Runtime ---'
    cat /etc/os-release | head -n 3

    echo
    echo '--- Executables ---'
    ls -lh \
        /usr/bin/mrcoordinator \
        /usr/bin/mrworker \
        /usr/bin/$SO_FILENAME

    echo
    echo '--- Plugin dependencies ---'
    ldd /usr/bin/$SO_FILENAME || true
"


# ============================================================
# Extract plugin for inspection
# ============================================================

log_info "Extracting plugin for inspection..."

PLUGIN_CONTAINER=$(docker create "$DOCKER_IMG")

if docker cp \
    "$PLUGIN_CONTAINER:/usr/bin/$SO_FILENAME" \
    "$TEMP_DIR/$SO_FILENAME"; then

    log_info "Plugin copied to:"
    log_info "  $TEMP_DIR/$SO_FILENAME"

else
    log_error "Could not extract plugin from image"
fi

docker rm "$PLUGIN_CONTAINER" >/dev/null


# ============================================================
# Create Docker network
# ============================================================

log_info "Creating Docker network:"
log_info "  $SHARED_NET_NAME"

if ! docker network create "$SHARED_NET_NAME"; then
    log_error "Failed to create Docker network"
    exit 1
fi


# ============================================================
# Start Coordinator
# ============================================================

log_info "Starting Coordinator"

#
# IMPORTANT:
#
# The ENTIRE TEMP_DIR is mounted into the container.
#
# This means that all workers share:
#
#   /mnt/temp/inputs
#   /mnt/temp/mr-0-0
#   /mnt/temp/mr-0-1
#   /mnt/temp/mr-1-0
#   ...
#
# This is required because MapReduce workers create intermediate
# files that Reduce workers need to consume.
#

COORDINATOR_CONTAINER_ID=$(docker run -d \
    --name "$COORDINATOR_NAME" \
    --user "$DOCKER_USER" \
    --network "$SHARED_NET_NAME" \
    -v "$TEMP_DIR:$SHARED_MNT_DIR" \
    --entrypoint /bin/bash \
    "$DOCKER_IMG" \
    -c "cd '$SHARED_MNT_DIR/outputs' && /usr/bin/mrcoordinator tcp://0.0.0.0:$MR_TCP_PORT \$(find ../inputs -type f)"
)


if [[ -z "$COORDINATOR_CONTAINER_ID" ]]; then
    log_error "Failed to start coordinator"
    exit 1
fi

ALL_CONTAINER_IDS+=("$COORDINATOR_CONTAINER_ID")

log_info "Coordinator container ID:"
log_info "  $COORDINATOR_CONTAINER_ID"

log_info "Coordinator container name:"
log_info "  $COORDINATOR_NAME"


# ============================================================
# Coordinator logs
# ============================================================

docker logs -f "$COORDINATOR_CONTAINER_ID" 2>&1 | \
    sed "s/^/[Coordinator][$COORDINATOR_NAME] /" &

LOG_PIDS+=("$!")


# ============================================================
# Wait for coordinator
# ============================================================

log_info "Waiting for $MR_WAIT_TIME seconds..."

sleep "$MR_WAIT_TIME"


# ============================================================
# Start Workers
# ============================================================

log_info "Starting $MR_NUM_WORKERS Workers"

for ((i=1; i<=MR_NUM_WORKERS; i++)); do

    WORKER_CONTAINER_NAME="mr-worker-$i-$(date +%s%N)"

    log_info "Starting Worker $i"


    #
    # IMPORTANT:
    #
    # Do NOT put comments inside this continued command.
    #
    # Every worker mounts the same TEMP_DIR.
    #

    WORKER_CONTAINER_ID=$(docker run -d \
        --name "$WORKER_CONTAINER_NAME" \
        --user "$DOCKER_USER" \
        --network "$SHARED_NET_NAME" \
        --restart on-failure \
        -v "$TEMP_DIR:$SHARED_MNT_DIR" \
        -e "TMPDIR=$SHARED_MNT_DIR/tmp" \
        --entrypoint /bin/bash \
        "$DOCKER_IMG" \
        -c "cd '$SHARED_MNT_DIR/outputs' && /usr/bin/mrworker /usr/bin/$SO_FILENAME tcp://$COORDINATOR_NAME:$MR_TCP_PORT"
    )


    if [[ -z "$WORKER_CONTAINER_ID" ]]; then
        log_error "Failed to start Worker $i"
        exit 1
    fi

    ALL_CONTAINER_IDS+=("$WORKER_CONTAINER_ID")


    log_info "Worker $i container ID:"
    log_info "  $WORKER_CONTAINER_ID"

    log_info "Worker $i container name:"
    log_info "  $WORKER_CONTAINER_NAME"


    sleep 1


    STATUS=$(docker inspect \
        -f '{{.State.Status}}' \
        "$WORKER_CONTAINER_ID")


    log_info "Worker $i container status:"
    log_info "  $STATUS"


    # if [[ "$STATUS" != "running" ]]; then

    #     log_error "Worker $i exited immediately"

    #     log_error "Worker $i logs:"
    #     docker logs "$WORKER_CONTAINER_ID" || true

    #     exit 1
    # fi


    # log_info "Worker $i is running"
    log_info "Worker $i has been started"


    docker logs -f "$WORKER_CONTAINER_ID" 2>&1 | \
        sed "s/^/[Worker $i][$WORKER_CONTAINER_NAME] /" &

    LOG_PIDS+=("$!")

done


# ============================================================
# Show shared directory
# ============================================================

log_info "Shared temporary directory:"
log_info "  $TEMP_DIR"

log_info "MapReduce job running..."


# ============================================================
# Wait for coordinator
# ============================================================

while true; do

    COORDINATOR_STATUS=$(docker inspect \
        -f '{{.State.Status}}' \
        "$COORDINATOR_CONTAINER_ID" 2>/dev/null || echo "missing")


    if [[ "$COORDINATOR_STATUS" == "exited" ||
          "$COORDINATOR_STATUS" == "dead" ||
          "$COORDINATOR_STATUS" == "missing" ]]; then
        break
    fi


    sleep 1

done


log_info "Coordinator has stopped."

exit 0
