# ========================================
# GPU Worker Dockerfile (memi 통합 버전)
# ========================================
FROM nvidia/cuda:11.8.0-cudnn8-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# ========================================
# 시스템 패키지 설치
# ========================================
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.10 \
    python3-pip \
    python3-dev \
    ffmpeg \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    libgl1-mesa-glx \
    git \
    wget \
    curl \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python 심볼릭 링크
RUN ln -sf /usr/bin/python3.10 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

WORKDIR /app

# ========================================
# Python 패키지 설치
# ========================================

# 1. 기본 패키지 (pip 업그레이드)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# 2. Cython (cocoapi 의존성)
RUN pip install --no-cache-dir Cython numpy

# 3. PyTorch + CUDA 11.8
RUN pip install --no-cache-dir \
    torch==2.1.2 \
    torchvision==0.16.2 \
    --index-url https://download.pytorch.org/whl/cu118

# 4. cocoapi (Cython 필요)
RUN PIP_NO_BUILD_ISOLATION=false pip install --no-cache-dir \
    "git+https://github.com/philferriere/cocoapi.git#subdirectory=PythonAPI"

# 5. GPU Worker 의존성
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ========================================
# memi 설치 (EFS에서 마운트할 예정)
# ========================================
# EFS 마운트 포인트: /mnt/efs/memi/
# 컨테이너 내부에서는 /app/memi로 심볼릭 링크

# ========================================
# GPU Worker 코드 복사
# ========================================
COPY . /app/gpu_worker/

# 작업 디렉토리 설정
WORKDIR /app/gpu_worker

# 임시 디렉토리 생성
RUN mkdir -p /app/temp /app/results

# ========================================
# 환경 변수 (EFS 경로)
# ========================================
ENV MEMI_PATH=/mnt/efs/memi
ENV DETECTOR_WEIGHTS=/mnt/efs/models/yolov8x_person_face.pt
ENV MIVOLO_CHECKPOINT=/mnt/efs/models/model_imdb_cross_person_4.24_99.46.pth.tar
ENV MEBOW_CFG=/mnt/efs/config/mebow.yaml
ENV VLM_PATH=/mnt/efs/checkpoints/llava-fastvithd_0.5b_stage2

# Django 설정
ENV DJANGO_SETTINGS_MODULE=core.settings
ENV PYTHONPATH=/app/gpu_worker:/mnt/efs/django:${PYTHONPATH}

# ========================================
# 헬스체크
# ========================================
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
    CMD python -c "import torch; torch.cuda.is_available()" || exit 1

# ========================================
# 실행
# ========================================
CMD ["python", "video_processor.py"]