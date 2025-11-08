"""
S3 비디오 업로드 API 뷰
JWT 인증 및 Pre-signed URL 기반 업로드
"""

from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.http import JsonResponse
import json
import logging
import os
import uuid

from datetime import datetime
from .services.s3_service import s3_service
# from .services.auth_service import jwt_required  # TODO: 임시 비활성화 (개발용)
from .services.sqs_service import sqs_service
from apps.db.models import Video
from apps.db.serializers import VideoSerializer

logger = logging.getLogger(__name__)


@api_view(['POST'])
# @jwt_required  # TODO: 임시 비활성화 (개발용)
def request_upload_url(request):
    """
    Step 1: 업로드 토큰 및 Pre-signed URL 요청

    Request Body:
    {
        "file_name": "video.mp4",
        "file_size": 1048576,
        "content_type": "video/mp4"
    }

    Response:
    {
        "upload_token": "jwt_token",
        "presigned_url": "https://s3.amazonaws.com/...",
        "s3_key": "videos/2024/01/15/uuid_video.mp4",
        "expires_in": 900
    }
    """
    try:
        data = request.data
        file_name = data.get('file_name')
        file_size = data.get('file_size')
        content_type = data.get('content_type', 'video/mp4')

        # 입력 검증
        if not file_name or not file_size:
            return Response(
                {'error': 'file_name과 file_size가 필요합니다.'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # 파일 크기 제한 (5GB)
        max_size = 5 * 1024 * 1024 * 1024
        if file_size > max_size:
            return Response(
                {'error': '파일 크기는 5GB를 초과할 수 없습니다.'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # 비디오 파일 타입 검증
        allowed_types = ['video/mp4', 'video/avi', 'video/mov', 'video/wmv']
        if content_type not in allowed_types:
            return Response(
                {'error': f'지원되지 않는 파일 타입입니다. 허용된 타입: {allowed_types}'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # JWT에서 사용자 ID 추출 (임시: 하드코딩)
        # user_id = request.user_payload['user_id']  # JWT 사용 시
        user_id = 'demo_user'  # TODO: 임시 사용자 ID
        
        # 업로드 토큰 생성
        upload_token = s3_service.generate_upload_token(
            user_id=user_id,
            file_name=file_name,
            file_size=file_size
        )
        
        # Pre-signed URL 생성
        presigned_url, s3_key = s3_service.generate_presigned_upload_url(
            token=upload_token,
            content_type=content_type
        )
        
        logger.info(f"📡 업로드 URL 요청 성공: user_id={user_id}, s3_key={s3_key}")
        
        return Response({
            'upload_token': upload_token,
            'presigned_url': presigned_url,
            's3_key': s3_key,
            'expires_in': 900  # 15분
        }, status=status.HTTP_200_OK)
        
    except ValueError as e:
        logger.error(f"❌ 업로드 URL 요청 실패: {e}")
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_400_BAD_REQUEST
        )
    except Exception as e:
        logger.error(f"❌ 서버 오류: {e}")
        return Response(
            {'error': '서버 내부 오류가 발생했습니다.'}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['POST'])
# @jwt_required  # TODO: 임시 비활성화 (개발용)
def confirm_upload(request):
    """
    Step 2: 업로드 완료 확인 및 비디오 메타데이터 저장
    
    Request Body:
    {
        "upload_token": "jwt_token",
        "s3_key": "videos/2024/01/15/uuid_video.mp4",
        "duration": 120.5,
        "thumbnail_url": "optional_thumbnail_url",
        "video_datetime": "2024-11-08T10:30:00Z"
    }
    
    Response:
    {
        "success": true,
        "video_id": 123,
        "message": "업로드가 완료되었습니다.",
        "video": { ... },
        "processing_info": {
            "trigger": "S3 Event Notification",
            "status": "pending",
            "note": "GPU 처리가 자동으로 시작됩니다"
        }
    }
    """
    try:
        data = request.data
        upload_token = data.get('upload_token')
        s3_key = data.get('s3_key')
        duration = data.get('duration', 0)
        thumbnail_url = data.get('thumbnail_url')
        video_datetime = data.get('video_datetime')  # 비디오 촬영 시간 추가

        # 입력 검증
        if not upload_token or not s3_key:
            return Response(
                {'error': 'upload_token과 s3_key가 필요합니다.'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # 업로드 토큰 검증
        token_payload = s3_service.validate_upload_token(upload_token)

        # S3에 파일이 실제로 업로드되었는지 확인
        if not s3_service.check_file_exists(s3_key):
            return Response(
                {'error': 'S3에서 파일을 찾을 수 없습니다. 업로드를 다시 시도해주세요.'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # 비디오 메타데이터 DB 저장
        try:
            logger.info(f"Video 데이터 준비: file_name={token_payload['file_name']}, s3_key={s3_key}")
            
            # thumbnail_url에서 S3 키만 추출 (전체 URL이 아닌)
            thumbnail_s3_key = ''
            if thumbnail_url:
                # URL에서 버킷명 이후의 키만 추출
                # 예: https://capstone-dev-thumbnails.s3.ap-northeast-2.amazonaws.com/thumbnails/2025/10/28/xxx.png
                # -> thumbnails/2025/10/28/xxx.png
                if 's3' in thumbnail_url and '.amazonaws.com/' in thumbnail_url:
                    thumbnail_s3_key = thumbnail_url.split('.amazonaws.com/')[-1]
                    # URL 인코딩된 부분 디코딩
                    from urllib.parse import unquote
                    thumbnail_s3_key = unquote(thumbnail_s3_key.split('?')[0])  # 쿼리 파라미터 제거
                else:
                    # 이미 키 형식이면 그대로 사용
                    thumbnail_s3_key = thumbnail_url
            
            logger.info(f"Thumbnail S3 key: {thumbnail_s3_key[:100]}...")  # 처음 100자만 로깅
            
            video_data = {
                'name': token_payload['file_name'],
                'filename': token_payload['file_name'],
                'original_filename': token_payload['file_name'],
                's3_key': s3_key,  # Primary S3 object key
                's3_raw_key': s3_key,  # S3 raw video key
                'file_size': token_payload['file_size'],
                'duration': duration,
                's3_thumbnail_key': thumbnail_s3_key,
                'thumbnail_s3_key': thumbnail_s3_key,
            }

            # 비디오 촬영 시간이 있으면 추가
            if video_datetime:
                video_data['recorded_at'] = video_datetime
            
            logger.info(f"Video.objects.create 호출 중...")
            video = Video.objects.create(**video_data)
            logger.info(f"Video 생성 성공: video_id={video.video_id}")
            
            serializer = VideoSerializer(video)
            logger.info(f"Serializer 생성 성공")
            
        except Exception as db_error:
            logger.error(f"DB 저장 실패: {type(db_error).__name__}: {str(db_error)}")
            logger.error(f"video_data: {video_data}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
        logger.info(f"비디오 업로드 완료: video_id={video.video_id}, s3_key={s3_key}")
        
        return Response({
            'success': True,
            'video_id': video.video_id,
            'message': '업로드가 완료되었습니다.',
            'video': serializer.data,
            'processing_queued': {
                'trigger': 'S3 Event Notification',
                'status': 'pending',
                'note': 'S3가 자동으로 SQS에 메시지를 발송하며, Lambda가 EC2 GPU 인스턴스를 시작합니다.',
            }
        }, status=status.HTTP_201_CREATED)
        
    except ValueError as e:
        logger.error(f"[ValueError] 업로드 확인 실패: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_400_BAD_REQUEST
        )
    except Exception as e:
        logger.error(f"[Exception] 서버 오류: {type(e).__name__}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return Response(
            {'error': f'서버 내부 오류: {type(e).__name__}: {str(e)}'}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
# @jwt_required  # TODO: 임시 비활성화 (개발용)
def get_video_download_url(request, video_id):
    """
    비디오 다운로드/스트리밍 URL 생성
    
    Response:
    {
        "download_url": "https://s3.amazonaws.com/...",
        "expires_in": 3600
    }
    """
    try:
        video = Video.objects.get(video_id=video_id)
        
        # S3 키에서 다운로드 URL 생성
        download_url = s3_service.generate_download_url(video.video_file)
        
        logger.info(f"📥 다운로드 URL 생성: video_id={video_id}")
        
        return Response({
            'download_url': download_url,
            'expires_in': 3600  # 1시간
        }, status=status.HTTP_200_OK)
        
    except Video.DoesNotExist:
        return Response(
            {'error': '존재하지 않는 비디오입니다.'}, 
            status=status.HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"❌ 다운로드 URL 생성 실패: {e}")
        return Response(
            {'error': '다운로드 URL 생성에 실패했습니다.'}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['DELETE'])
# @jwt_required  # TODO: 임시 비활성화 (개발용)
def delete_video(request, video_id):
    """
    비디오 삭제 (DB + S3)
    """
    try:
        video = Video.objects.get(video_id=video_id)
        s3_key = video.video_file
        
        # S3에서 파일 삭제
        s3_deleted = s3_service.delete_video(s3_key)
        
        # DB에서 비디오 삭제
        video.delete()
        
        logger.info(f"🗑️ 비디오 삭제 완료: video_id={video_id}, s3_deleted={s3_deleted}")
        
        return Response({
            'message': '비디오가 삭제되었습니다.',
            's3_deleted': s3_deleted
        }, status=status.HTTP_200_OK)
        
    except Video.DoesNotExist:
        return Response(
            {'error': '존재하지 않는 비디오입니다.'}, 
            status=status.HTTP_404_NOT_FOUND
        )
    except Exception as e:
        logger.error(f"❌ 비디오 삭제 실패: {e}")
        return Response(
            {'error': '비디오 삭제에 실패했습니다.'}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['POST'])
@csrf_exempt
def upload_thumbnail(request):
    """
    썸네일 업로드 (S3 thumbnails 버킷)
    
    Request:
    - multipart/form-data
    - thumbnail: File (이미지 파일)
    - fileName: string (파일명)
    
    Response:
    {
        "success": true,
        "thumbnail_url": "https://s3.../thumbnails/xxx.png",
        "s3_key": "thumbnails/xxx.png"
    }
    """
    try:
        if 'thumbnail' not in request.FILES:
            return Response(
                {'error': '썸네일 파일이 필요합니다.'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        thumbnail_file = request.FILES['thumbnail']
        file_name = request.POST.get('fileName', thumbnail_file.name)
        
        # 파일명에서 확장자 추출
        file_ext = file_name.split('.')[-1] if '.' in file_name else 'png'
        
        # S3 키 생성 (thumbnails/YYYY/MM/DD/uuid_filename.ext)
        now = datetime.now()
        s3_key = f"thumbnails/{now.year}/{now.month:02d}/{now.day:02d}/{uuid.uuid4()}_{file_name}"
        
        # thumbnails 버킷 설정
        thumbnails_bucket = os.environ.get('AWS_S3_THUMBNAILS_BUCKET', 'capstone-dev-thumbnails')
        
        # S3에 업로드
        s3_client = s3_service.s3_client
        s3_client.upload_fileobj(
            thumbnail_file,
            thumbnails_bucket,
            s3_key,
            ExtraArgs={
                'ContentType': thumbnail_file.content_type or 'image/png',
                'ACL': 'private'
            }
        )
        
        # 썸네일 URL 생성 (Pre-signed URL, 7일 유효)
        thumbnail_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': thumbnails_bucket,
                'Key': s3_key
            },
            ExpiresIn=7 * 24 * 3600  # 7일
        )
        
        logger.info(f"🖼️ 썸네일 업로드 완료: s3_key={s3_key}, bucket={thumbnails_bucket}")
        
        return Response({
            'success': True,
            'thumbnail_url': thumbnail_url,
            's3_key': s3_key
        }, status=status.HTTP_201_CREATED)
        
    except Exception as e:
        logger.error(f"❌ 썸네일 업로드 실패: {e}")
        return Response(
            {'error': f'썸네일 업로드에 실패했습니다: {str(e)}'}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
