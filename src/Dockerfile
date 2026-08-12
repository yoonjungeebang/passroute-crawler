# 베이스 이미지에 ONNX 모델 + 의존성이 포함되어 있다.
# 이 Dockerfile은 소스 코드만 복사한다.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

COPY src/ ${LAMBDA_TASK_ROOT}/

CMD ["app.job_detail_crawler"]
