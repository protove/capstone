# ============================================
# S3 버킷들
# ============================================

# 1. 원본 영상 저장 버킷 (Raw Videos)
resource "aws_s3_bucket" "raw_videos" {
  bucket = "capstone-${var.environment}-raw"

  tags = {
    Name        = "capstone-raw-videos"
    Environment = var.environment
    Project     = "Unmanned"
  }
}

# 2. 썸네일 이미지 저장 버킷 (Thumbnails)
resource "aws_s3_bucket" "thumbnails" {
  bucket = "capstone-${var.environment}-thumbnails"

  tags = {
    Name        = "capstone-thumbnails"
    Environment = var.environment
    Project     = "Unmanned"
  }
}

# 3. 하이라이트 프레임 저장 버킷 (Highlights)
resource "aws_s3_bucket" "highlights" {
  bucket = "capstone-${var.environment}-highlights"

  tags = {
    Name        = "capstone-highlights"
    Environment = var.environment
    Project     = "Unmanned"
  }
}

# ============================================
# S3 버킷 설정
# ============================================

# Raw Videos - Versioning
resource "aws_s3_bucket_versioning" "raw_videos" {
  bucket = aws_s3_bucket.raw_videos.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Raw Videos - Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "raw_videos" {
  bucket = aws_s3_bucket.raw_videos.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Raw Videos - Public Access Block
resource "aws_s3_bucket_public_access_block" "raw_videos" {
  bucket = aws_s3_bucket.raw_videos.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Raw Videos - CORS Configuration
resource "aws_s3_bucket_cors_configuration" "raw_videos" {
  bucket = aws_s3_bucket.raw_videos.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "PUT", "POST", "DELETE", "HEAD"]
    allowed_origins = [
      "https://deepsentinel.cloud",
      "https://www.deepsentinel.cloud",
      "https://api.deepsentinel.cloud",
      "http://localhost:3000",
      "http://localhost:8000"
    ]
    expose_headers  = ["ETag", "x-amz-server-side-encryption", "x-amz-request-id"]
    max_age_seconds = 3000
  }
}

# ============================================
# S3 Event Notification --> S3
# ============================================

# S3 Event Notification 설정
resource "aws_s3_bucket_notification" "raw_videos_notification" {
  bucket = aws_s3_bucket.raw_videos.id

  # SQS 큐로 이벤트 전송
  queue {
    queue_arn = aws_sqs_queue.video_processing.arn
    
    events = ["s3:ObjectCeated:*"]
    filter_prefix = "video/"
    filter_suffix = ".mp4" #필요시 추가할 필요있음
  }

  depends_on = [aws_sqs_queue_policy.s3_to_sqs]
}

# SQS Queue Policy
resource "aws_sqs_queue_policy" "s3_to_sqs" {
  queue_url = aws_sqs_queue.video_processing.url
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid = "AllowS3ToSendMessageToSQS"
        Effect = "Allow"

        Principal = {
          Service = "s3.amazonaws.com"
        }

        Action = "sqs:SendMessage

        Resource = aws_sqs_queue.video_processing.arn

        Condition = {
          ArnEquals = {
            "aws:SorceArn" = aws_s3_bucket.raw_videos.arn
          }
        }
      }
    ]
  })
}

# Thumbnails - Versioning
resource "aws_s3_bucket_versioning" "thumbnails" {
  bucket = aws_s3_bucket.thumbnails.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Thumbnails - Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "thumbnails" {
  bucket = aws_s3_bucket.thumbnails.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Thumbnails - Public Access Block
resource "aws_s3_bucket_public_access_block" "thumbnails" {
  bucket = aws_s3_bucket.thumbnails.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Thumbnails - CORS Configuration
resource "aws_s3_bucket_cors_configuration" "thumbnails" {
  bucket = aws_s3_bucket.thumbnails.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "PUT", "POST", "DELETE", "HEAD"]
    allowed_origins = [
      "https://deepsentinel.cloud",
      "https://www.deepsentinel.cloud",
      "https://api.deepsentinel.cloud",
      "http://localhost:3000",
      "http://localhost:8000"
    ]
    expose_headers  = ["ETag", "x-amz-server-side-encryption", "x-amz-request-id"]
    max_age_seconds = 3000
  }
}

# Highlights - Versioning
resource "aws_s3_bucket_versioning" "highlights" {
  bucket = aws_s3_bucket.highlights.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Highlights - Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "highlights" {
  bucket = aws_s3_bucket.highlights.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Highlights - Public Access Block
resource "aws_s3_bucket_public_access_block" "highlights" {
  bucket = aws_s3_bucket.highlights.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Highlights - CORS Configuration
resource "aws_s3_bucket_cors_configuration" "highlights" {
  bucket = aws_s3_bucket.highlights.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = [
      "https://deepsentinel.cloud",
      "https://www.deepsentinel.cloud",
      "https://api.deepsentinel.cloud",
      "http://localhost:3000",
      "http://localhost:8000"
    ]
    expose_headers  = ["ETag", "x-amz-server-side-encryption", "x-amz-request-id"]
    max_age_seconds = 3000
  }
}

# ============================================
# Outputs
# ============================================
output "s3_raw_videos_bucket" {
  description = "Raw videos S3 bucket name"
  value       = aws_s3_bucket.raw_videos.id
}

output "s3_raw_videos_arn" {
  description = "Raw video S3 bucket ARN"
  value = aws_s3_bucket.raw_video.arn
}

output "s3_thumbnails_bucket" {
  description = "Thumbnails S3 bucket name"
  value       = aws_s3_bucket.thumbnails.id
}

output "s3_highlights_bucket" {
  description = "Highlights S3 bucket name"
  value = aws_s3_bucket.highlights.id
}
