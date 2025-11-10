"""
EC2 GPU Video Processing Worker
Lambda 트리거로 시작되는 One-Shot 모드 워커
SQS Long Polling을 통한 비디오 처리
"""

import os
import sys
import json
import time
import logging
import signal
import boto3
import traceback
import subprocess
import requests
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional
from visibility_manager import VisibilityTimeoutManager
from error_handler import retry_manager, error_tracker, retry_on_error, safe_execute


# Django 설정을 위한 경로 추가
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DJANGO_ROOT = PROJECT_ROOT / 'back'

sys.path.insert(0, str(DJANGO_ROOT))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

try:
    import django
    django.setup()
    
    from apps.api.services.sqs_service import sqs_service
    from apps.api.services.s3_service import s3_service
    from apps.db.models import Video
    
    print("Django 모듈 로드 완료")
except Exception as e:
    print(f"Django 모듈 로드 실패: {e}")
    sys.exit(1)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(SCRIPT_DIR / 'gpu_worker.log')
    ]
)
logger = logging.getLogger('GPUWorker')


class GPUVideoWorker:
    """
    EC2 GPU 비디오 처리 워커 (Lambda 트리거 One-Shot 모드)
    """
    
    def __init__(self):
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        self._stop_event = threading.Event()
        
        # 가시성 타임아웃 매니저 초기화
        self.visibility_manager = VisibilityTimeoutManager(sqs_service)
        
        # 시그널 핸들러 등록 (Graceful Shutdown)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # ========================================
        # 실행 모드 설정
        # ========================================
        # ONE_SHOT_MODE: Lambda가 EC2를 시작하면 True
        # CONTINUOUS: 개발/테스트용 (계속 실행)
        self.one_shot_mode = os.environ.get('ONE_SHOT_MODE', 'true').lower() == 'true'
        self.instance_id = self._get_instance_id()
        
        # ========================================
        # memi 경로 설정 (EFS 마운트)
        # ========================================
        self.memi_path = Path(os.environ.get('MEMI_PATH', '/mnt/efs/memi'))
        self.memi_run_script = self.memi_path / 'run.py'
        
        # memi 설정 경로
        self.detector_weights = os.environ.get(
            'DETECTOR_WEIGHTS', 
            '/mnt/efs/models/yolov8x_person_face.pt'
        )
        self.mivolo_checkpoint = os.environ.get(
            'MIVOLO_CHECKPOINT', 
            '/mnt/efs/models/model_imdb_cross_person_4.24_99.46.pth.tar'
        )
        self.mebow_cfg = os.environ.get(
            'MEBOW_CFG', 
            '/mnt/efs/config/mebow.yaml'
        )
        self.vlm_path = os.environ.get(
            'VLM_PATH', 
            '/mnt/efs/checkpoints/llava-fastvithd_0.5b_stage2'
        )
        
        # memi 설치 확인
        if not self.memi_run_script.exists():
            logger.error(f"memi run.py not found at: {self.memi_run_script}")
            logger.error("Please ensure EFS is mounted and memi is installed")
            raise FileNotFoundError(f"memi not found: {self.memi_run_script}")
        
        logger.info("=" * 60)
        logger.info("GPU Video Worker 초기화")
        logger.info("=" * 60)
        logger.info(f"   실행 모드: {'ONE-SHOT (Lambda 트리거)' if self.one_shot_mode else 'CONTINUOUS (개발용)'}")
        logger.info(f"   Instance ID: {self.instance_id}")
        logger.info(f"   memi 경로: {self.memi_path}")
        logger.info(f"   Detector: {Path(self.detector_weights).name}")
        logger.info(f"   MiVOLO: {Path(self.mivolo_checkpoint).name}")
        logger.info(f"   MEBOW: {Path(self.mebow_cfg).name}")
        logger.info(f"   VLM: {Path(self.vlm_path).name}")
        logger.info("=" * 60)
    
    def _get_instance_id(self) -> str:
        """
        EC2 인스턴스 ID 조회
        
        Returns:
            인스턴스 ID (예: i-0123456789abcdef0)
        """
        try:
            # 환경 변수에서 먼저 확인
            instance_id = os.environ.get('INSTANCE_ID')
            if instance_id:
                return instance_id
            
            # EC2 메타데이터 서비스에서 조회
            response = requests.get(
                'http://169.254.169.254/latest/meta-data/instance-id',
                timeout=2
            )
            
            if response.status_code == 200:
                return response.text.strip()
            
            logger.warning("인스턴스 ID 조회 실패, 기본값 사용")
            return 'unknown'
            
        except Exception as e:
            logger.warning(f"인스턴스 ID 조회 오류: {e}")
            return 'unknown'
    
    def _signal_handler(self, signum, frame):
        """시그널 핸들러 - Graceful Shutdown"""
        logger.info(f"시그널 {signum} 수신 - 워커 종료 중...")
        self.running = False
        self._stop_event.set()
        self._print_final_statistics()
    
    def _print_final_statistics(self):
        """최종 통계 및 오류 요약 출력"""
        logger.info("=" * 60)
        logger.info("GPU Video Worker 최종 통계")
        logger.info("=" * 60)
        
        # 기본 통계
        logger.info(f"   처리 성공: {self.processed_count}건")
        logger.info(f"   처리 실패: {self.error_count}건")
        
        total_messages = self.processed_count + self.error_count
        if total_messages > 0:
            success_rate = (self.processed_count / total_messages) * 100
            logger.info(f"   성공률: {success_rate:.1f}%")
        
        # 오류 통계
        error_summary = error_tracker.get_error_summary()
        if error_summary['total_errors'] > 0:
            logger.info(f"      오류 요약:")
            logger.info(f"      전체 오류: {error_summary['total_errors']}건")
            logger.info(f"      오류 타입: {error_summary['error_types']}개")
            logger.info(f"      최다 오류: {error_summary['most_common_error']}")
        
        logger.info("=" * 60)
    
    def start_worker_loop(self):
        """
        메인 워커 루프 시작
        Lambda 트리거 시 One-Shot 모드로 실행
        """
        logger.info("GPU Video Worker 시작...")
        
        # 가시성 타임아웃 모니터링 시작
        self.visibility_manager.start_monitoring()
        
        try:
            if self.one_shot_mode:
                logger.info("ONE-SHOT 모드 실행 (Lambda 트리거)")
                self._process_one_shot()
            else:
                logger.info("CONTINUOUS 모드 실행 (개발용)")
                self._process_continuous()
                
        finally:
            # 가시성 타임아웃 모니터링 중지
            self.visibility_manager.stop_monitoring()
            
            # 최종 통계 출력
            self._print_final_statistics()
            logger.info("🏁 GPU Video Worker 완전 종료")
    
    def _process_one_shot(self):
        """
        One-Shot 모드: 큐의 모든 메시지 처리 후 자동 종료
        
        플로우:
        1. SQS에서 메시지 폴링 (20초 Long Polling)
        2. 메시지가 있으면 처리
        3. 3번 연속 빈 큐면 종료
        4. EC2 인스턴스 자동 종료
        """
        consecutive_empty_polls = 0
        max_empty_polls = 3  # 3번 연속 빈 큐면 종료
        
        logger.info("=" * 60)
        logger.info("⚡ One-Shot 처리 시작 (Lambda 트리거 모드)")
        logger.info(f"   Instance ID: {self.instance_id}")
        logger.info(f"   최대 빈 폴링 횟수: {max_empty_polls}회")
        logger.info("=" * 60)
        
        self.running = True
        
        while consecutive_empty_polls < max_empty_polls and not self._stop_event.is_set():
            try:
                # SQS에서 메시지 수신 (Long Polling 20초)
                logger.debug("📥 SQS 메시지 폴링 중... (20초 대기)")
                messages = sqs_service.receive_messages(
                    max_messages=1,
                    wait_time_seconds=20,
                    visibility_timeout=300  # 5분
                )
                
                if not messages:
                    consecutive_empty_polls += 1
                    logger.info(f"📭 메시지 없음 ({consecutive_empty_polls}/{max_empty_polls})")
                    
                    if consecutive_empty_polls >= max_empty_polls:
                        logger.info("✅ 모든 메시지 처리 완료. 종료합니다.")
                        break
                    
                    continue
                
                # 메시지가 있으면 카운터 리셋
                consecutive_empty_polls = 0
                logger.info(f"📨 메시지 수신: {len(messages)}개")
                
                # 메시지 처리
                for message in messages:
                    if self._stop_event.is_set():
                        logger.info("⏹️  중지 신호 수신, 루프 종료")
                        break
                    
                    try:
                        self._process_message_with_visibility_management(message)
                    except Exception as e:
                        logger.error(f"❌ 메시지 처리 실패: {e}")
                        error_tracker.record_error(
                            e,
                            context="One-Shot 메시지 처리",
                            function_name="_process_one_shot"
                        )
                
            except KeyboardInterrupt:
                logger.info("⌨️  사용자 중단 (Ctrl+C)")
                break
            except Exception as e:
                logger.error(f"❌ One-Shot 루프 오류: {e}", exc_info=True)
                time.sleep(10)
    
        # ✨ 개선: EC2 자동 종료 (One-Shot 모드에서만)
        if self.one_shot_mode and self.instance_id != 'unknown':
            logger.info("=" * 60)
            logger.info("🔌 One-Shot 모드 완료 - EC2 인스턴스 자동 종료")
            logger.info("=" * 60)
            self._stop_ec2_instance()
        else:
            logger.info("⚠️  EC2 자동 종료 건너뜀 (instance_id={})".format(self.instance_id))
    
    def _process_continuous(self):
        """
        Continuous 모드: 계속 실행 (개발/테스트용)
        
        주의: 프로덕션에서는 사용하지 마세요! (비용 발생)
        """
        logger.warning("CONTINUOUS 모드는 개발/테스트용입니다!")
        logger.warning("프로덕션에서는 ONE_SHOT_MODE=true를 사용하세요!")
        
        consecutive_empty_polls = 0
        max_empty_polls = 3
        
        self.running = True
        
        while self.running:
            try:
                # SQS Long Polling으로 메시지 수신
                logger.debug("📥 SQS 메시지 폴링 중... (20초 대기)")
                messages = sqs_service.receive_messages(
                    max_messages=1,
                    wait_time_seconds=20,
                    visibility_timeout=300
                )
                
                if messages:
                    consecutive_empty_polls = 0
                    for message in messages:
                        if not self.running:
                            break
                        self._process_message_with_visibility_management(message)
                else:
                    consecutive_empty_polls += 1
                    logger.debug(f"📭 메시지 없음 ({consecutive_empty_polls}/{max_empty_polls})")
                    
                    # 연속으로 빈 메시지가 여러 번 나오면 잠시 대기
                    if consecutive_empty_polls >= max_empty_polls:
                        logger.info("😴 잠시 대기 중... (30초)")
                        time.sleep(30)
                        consecutive_empty_polls = 0
            
            except KeyboardInterrupt:
                logger.info("⌨️  사용자 중단 (Ctrl+C)")
                break
            except Exception as e:
                logger.error(f"❌ 워커 루프 오류: {e}")
                self.error_count += 1
                time.sleep(10)
    
    def _stop_ec2_instance(self):
        """
        현재 EC2 인스턴스를 자동으로 종료
        
        주의:
        - IAM 역할에 ec2:StopInstances 권한 필요
        - One-Shot 모드에서만 호출됨
        """
        try:
            if self.instance_id == 'unknown':
                logger.error("인스턴스 ID를 알 수 없어 종료할 수 없습니다.")
                logger.error("수동으로 EC2 콘솔에서 인스턴스를 종료하세요!")
                return
            
            logger.info(f"EC2 인스턴스 종료 시작: {self.instance_id}")
            
            ec2_client = boto3.client(
                'ec2',
                region_name=os.environ.get('AWS_REGION', 'ap-northeast-2')
            )
            
            # 인스턴스 종료 명령
            response = ec2_client.stop_instances(InstanceIds=[self.instance_id])
            
            stopping_instances = response['StoppingInstances']
            if stopping_instances:
                previous_state = stopping_instances[0]['PreviousState']['Name']
                current_state = stopping_instances[0]['CurrentState']['Name']
                logger.info(f"EC2 인스턴스 종료 명령 전송 완료")
                logger.info(f"   이전 상태: {previous_state}")
                logger.info(f"   현재 상태: {current_state}")
            else:
                logger.warning("인스턴스 종료 응답이 비어있음")
            
        except Exception as e:
            logger.error(f"EC2 종료 실패: {type(e).__name__}: {str(e)}")
            logger.error("수동으로 EC2 콘솔에서 인스턴스를 종료하세요!")
            logger.error(f"   Instance ID: {self.instance_id}")
    
    def _process_message_with_visibility_management(self, message: Dict[str, Any]):
        """
        SQS 메시지 처리 (가시성 타임아웃 자동 관리 + 오류 처리)
        
        Args:
            message: SQS 메시지
        """
        receipt_handle = message.get('ReceiptHandle')
        message_body = message.get('Body', '{}')
        
        try:
            # S3 Event 파싱
            success, payload = safe_execute(
                self._parse_message_body,
                message,
                context=f"메시지 파싱 (handle={receipt_handle[:10]}...)"
            )
            
            if not success:
                logger.error(f"❌ 메시지 파싱 실패: {payload}")
                sqs_service.delete_message(receipt_handle)
                self.error_count += 1
                return
            
            # S3 Event 형식에서 정보 추출
            video_id = payload.get('video_id')
            s3_bucket = payload.get('bucket')
            s3_key = payload.get('key')
            
            logger.info(f"📋 메시지 처리 시작:")
            logger.info(f"   video_id: {video_id}")
            logger.info(f"   s3_bucket: {s3_bucket}")
            logger.info(f"   s3_key: {s3_key}")
            
            # 필수 정보 검증
            if not all([s3_bucket, s3_key]):
                error_msg = f"필수 정보 누락: bucket={s3_bucket}, key={s3_key}"
                logger.error(f"❌ {error_msg}")
                sqs_service.delete_message(receipt_handle)
                self.error_count += 1
                return
            
            # video_id가 없으면 경고 (S3 키로 처리 가능)
            if not video_id:
                logger.warning(f"⚠️  video_id 없음, S3 키로 처리: {s3_key}")
            
            # 파일 크기 기반 예상 처리 시간 계산
            estimated_time = self._estimate_processing_time_safe(s3_key)
            
            # 가시성 타임아웃 관리 시작
            self.visibility_manager.register_message(
                receipt_handle,
                video_id or s3_key,
                estimated_time
            )
            
            # 비디오 처리 실행 (재시도 로직 포함)
            processing_result = self._process_video_with_retry(
                video_id, 
                s3_bucket, 
                s3_key
            )
            
            if processing_result['success']:
                # 처리 완료 - 메시지 삭제
                success, _ = safe_execute(
                    sqs_service.delete_message,
                    receipt_handle,
                    context=f"메시지 삭제 video_id={video_id}"
                )
                
                self.visibility_manager.unregister_message(receipt_handle, 'completed')
                
                if success:
                    self.processed_count += 1
                    logger.info(f"✅ 비디오 처리 완료: video_id={video_id}")
                else:
                    logger.warning(f"⚠️  처리 성공, 메시지 삭제 실패: video_id={video_id}")
                    
            else:
                # 처리 실패
                error_type = processing_result.get('error_type', 'unknown')
                
                if error_type == 'permanent':
                    # 영구적 오류 - 메시지 삭제
                    logger.error(f"❌ 영구적 오류, 메시지 삭제: video_id={video_id}")
                    sqs_service.delete_message(receipt_handle)
                else:
                    # 일시적 오류 - 재처리 대기
                    logger.warning(f"⚠️  일시적 오류, 재처리 대기: video_id={video_id}")
                    safe_execute(
                        sqs_service.change_message_visibility,
                        receipt_handle,
                        0,  # 즉시 가시성 복구
                        context=f"가시성 복구 video_id={video_id}"
                    )
                
                self.visibility_manager.unregister_message(receipt_handle, 'failed')
                self.error_count += 1
                
        except Exception as e:
            # 예상치 못한 오류
            logger.error(f"❌ 메시지 처리 중 예상치 못한 오류: {e}")
            error_tracker.record_error(
                e,
                context=f"메시지 처리 handle={receipt_handle[:10]}...",
                function_name="_process_message_with_visibility_management"
            )
            
            # 가시성 복구
            try:
                sqs_service.change_message_visibility(receipt_handle, 0)
                self.visibility_manager.unregister_message(receipt_handle, 'error')
            except:
                pass
                
            self.error_count += 1
    
    def _estimate_processing_time_safe(self, s3_key: str) -> int:
        """
        S3 키를 기반으로 예상 처리 시간 계산
        
        Args:
            s3_key: S3 객체 키
            
        Returns:
            예상 처리 시간 (초)
        """
        try:
            # S3에서 파일 크기 정보 조회
            success, file_info = safe_execute(
                s3_service.get_file_info,
                s3_key,
                context=f"파일 정보 조회 {s3_key}"
            )
            
            if success and file_info:
                file_size = file_info.get('ContentLength', 0)
                # 파일 크기 기반 예상 시간 (MB당 1초 + 기본 120초)
                size_mb = file_size / (1024 * 1024)
                estimated_time = max(120, int(size_mb * 1.0 + 120))
                logger.debug(f"📊 파일 크기 기반 예상 시간: {size_mb:.1f}MB → {estimated_time}초")
                return estimated_time
        except Exception as e:
            logger.warning(f"⚠️  파일 크기 조회 실패, 기본값 사용: {e}")
        
        # 파일 확장자 기반 기본 예상 시간
        if s3_key.lower().endswith(('.mp4', '.avi', '.mov', '.mkv')):
            return 600  # 비디오: 10분
        elif s3_key.lower().endswith(('.jpg', '.png', '.jpeg', '.gif')):
            return 120  # 이미지: 2분
        else:
            return 300  # 기본: 5분
    
    def _process_video_with_retry(self, video_id: str, s3_bucket: str, s3_key: str) -> Dict[str, Any]:
        """
        비디오 처리 실행 (재시도 로직 포함)
        
        Args:
            video_id: 비디오 ID
            s3_bucket: S3 버킷명  
            s3_key: S3 객체 키
            
        Returns:
            처리 결과 딕셔너리
        """
        context = f"비디오 처리 video_id={video_id}"
        
        try:
            # 재시도 로직으로 비디오 처리 실행
            result = retry_manager.retry_with_backoff(
                self._process_video,
                video_id,
                s3_bucket,
                s3_key,
                context=context
            )
            return result
            
        except Exception as e:
            error_type = retry_manager.classify_error(e)
            logger.error(f"{context} 최종 실패: {type(e).__name__}: {str(e)}")
            
            return {
                'success': False,
                'error': str(e),
                'error_type': error_type.value
            }
    
    @retry_on_error(max_retries=2, context="S3 파일 다운로드")
    def _download_video_from_s3(self, s3_bucket: str, s3_key: str, local_path: str) -> bool:
        """
        S3에서 비디오 파일 다운로드 (재시도 기능 포함)
        
        Args:
            s3_bucket: S3 버킷명
            s3_key: S3 객체 키  
            local_path: 로컬 저장 경로
            
        Returns:
            다운로드 성공 여부
        """
        logger.info(f"S3 다운로드: s3://{s3_bucket}/{s3_key} → {local_path}")
        
        # S3 다운로드 실행
        s3_service.download_file(s3_bucket, s3_key, local_path)
        
        # 파일 존재 확인
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"다운로드된 파일을 찾을 수 없습니다: {local_path}")
        
        file_size = os.path.getsize(local_path)
        logger.info(f"다운로드 완료: {file_size:,} bytes")
        return True
    
    @retry_on_error(max_retries=2, context="분석 결과 업로드")  
    def _upload_results_to_s3(self, results: Dict, s3_bucket: str, s3_key: str) -> bool:
        """
        분석 결과를 S3에 업로드 (재시도 기능 포함)
        
        Args:
            results: 분석 결과 딕셔너리
            s3_bucket: S3 버킷명
            s3_key: S3 객체 키
            
        Returns:
            업로드 성공 여부
        """
        logger.info(f"분석 결과 업로드: {s3_key}")
        
        # JSON 직렬화
        results_json = json.dumps(results, ensure_ascii=False, indent=2)
        
        # S3 업로드
        s3_service.upload_string_as_file(results_json, s3_bucket, s3_key)
        
        logger.info(f"업로드 완료: s3://{s3_bucket}/{s3_key}")
        return True
    
    def _process_video(self, video_id: str, s3_bucket: str, s3_key: str) -> Dict[str, Any]:
        """
        비디오 GPU 처리 파이프라인 (오류 처리 강화)
        
        1. S3에서 비디오 다운로드
        2. GPU 추론 실행  
        3. 결과 저장
        4. Django API 상태 업데이트
        """
        local_video_path = None
        
        try:
            # Step 1: S3에서 비디오 다운로드 (재시도 포함)
            logger.info(f" S3 비디오 다운로드 시작: {s3_key}")
            local_video_path = self._download_video_safe(video_id, s3_bucket, s3_key)
            
            # Step 2: GPU 추론 실행 (재시도 포함)
            logger.info(f" GPU 추론 시작: {local_video_path}")
            inference_result = self._run_gpu_inference_safe(video_id, local_video_path)
            
            # Step 3: 결과 저장 (재시도 포함)
            logger.info(f" 처리 결과 저장 중...")
            storage_result = self._save_processing_result_safe(video_id, inference_result)
            
            # Step 4: Django DB 상태 업데이트 (재시도 포함)
            logger.info(f" DB 상태 업데이트 중...")
            self._update_video_status_safe(video_id, 'completed', inference_result)
            
            logger.info(f" 비디오 처리 완료: video_id={video_id}")
            
            return {
                'success': True,
                'video_id': video_id,
                'result': inference_result
            }
        
        except Exception as e:
            logger.error(f" 비디오 처리 오류: video_id={video_id}, error={type(e).__name__}: {str(e)}")
            
            # 실패 상태로 DB 업데이트 시도
            success, _ = safe_execute(
                self._update_video_status_safe,
                video_id, 
                'failed', 
                {'error': str(e), 'timestamp': datetime.now(timezone.utc).isoformat()},
                context=f"실패 상태 업데이트 video_id={video_id}"
            )
            
            if not success:
                logger.warning(f"⚠️ 실패 상태 DB 업데이트도 실패: video_id={video_id}")
            
            return {
                'success': False,
                'error': str(e),
                'video_id': video_id
            }
        
        finally:
            # 임시 파일 정리 (항상 실행)
            if local_video_path:
                safe_execute(
                    self._cleanup_temp_files,
                    local_video_path,
                    context=f"임시 파일 정리 video_id={video_id}"
                )
    
    def _download_video_safe(self, video_id: str, s3_bucket: str, s3_key: str) -> str:
        """S3에서 비디오 다운로드 (오류 처리 강화)"""
        # 임시 디렉토리 생성
        temp_dir = SCRIPT_DIR / 'temp'
        temp_dir.mkdir(exist_ok=True)
        
        # 로컬 파일 경로 생성
        file_extension = Path(s3_key).suffix or '.mp4'
        local_filename = f"video_{video_id}_{int(time.time())}{file_extension}"
        local_video_path = temp_dir / local_filename
        
        # 재시도 로직으로 다운로드 실행
        self._download_video_from_s3(s3_bucket, s3_key, str(local_video_path))
        
        return str(local_video_path)
    
    def _run_gpu_inference_safe(self, video_id: str, local_video_path: str) -> Dict[str, Any]:
        """GPU 추론 실행 (오류 처리 강화)"""
        context = f"GPU 추론 video_id={video_id}"
        
        try:
            # GPU 추론 실행 (재시도 포함)
            result = retry_manager.retry_with_backoff(
                self._run_gpu_inference,
                local_video_path,
                context=context
            )
            return result
            
        except Exception as e:
            logger.error(f" {context} 실패: {type(e).__name__}: {str(e)}")
            raise
    
    def _save_processing_result_safe(self, video_id: str, inference_result: Dict) -> bool:
        """처리 결과 저장 (오류 처리 강화)"""
        context = f"결과 저장 video_id={video_id}"
        
        try:
            # 결과 저장 실행 (재시도 포함)
            return retry_manager.retry_with_backoff(
                self._save_processing_result,
                video_id,
                inference_result,
                context=context
            )
            
        except Exception as e:
            logger.error(f" {context} 실패: {type(e).__name__}: {str(e)}")
            raise
    
    def _update_video_status_safe(self, video_id: str, status: str, data: Dict = None) -> bool:
        """비디오 상태 업데이트 (오류 처리 강화)"""
        context = f"상태 업데이트 video_id={video_id} status={status}"
        
        try:
            # 상태 업데이트 실행 (재시도 포함)
            return retry_manager.retry_with_backoff(
                self._update_video_status,
                video_id,
                status,
                data,
                context=context
            )
            
        except Exception as e:
            logger.error(f" {context} 실패: {type(e).__name__}: {str(e)}")
            raise
    
    def _update_video_status(self, video_id: str, status: str, data: Dict = None) -> bool:
        """
        비디오 상태 업데이트 (Django DB)
        """
        try:
            video = Video.objects.get(video_id=video_id)
            
            if status == 'completed':
                video.processing_status = 'completed'
                video.analysis_status = 'completed'
                video.analyzed_at = datetime.now(timezone.utc)
                if data:
                    video.major_event = data
            elif status == 'failed':
                video.processing_status = 'failed'
                video.analysis_status = 'failed'
                if data:
                    video.error_info = data
            
            video.save()
            logger.info(f"✅ 비디오 상태 업데이트: video_id={video_id}, status={status}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 상태 업데이트 실패: {e}")
            raise
    
    def _run_gpu_inference(self, video_path: str) -> Dict[str, Any]:
        """
        GPU 추론 실행 - memi run.py 호출
        
        Args:
            video_path: 로컬 비디오 파일 경로
        
        Returns:
            memi 분석 결과
        """
        logger.info(f"memi GPU 추론 시작: {video_path}")
        
        try:
            # 출력 디렉토리 생성
            output_dir = SCRIPT_DIR / 'results' / f"video_{int(time.time())}"
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # memi 명령어 구성
            cmd = [
                'python',
                str(self.memi_run_script),
                '--input', video_path,
                '--output', str(output_dir),
                '--detector-weights', self.detector_weights,
                '--checkpoint', self.mivolo_checkpoint,
                '--mebow-cfg', self.mebow_cfg,
                '--vlm-path', self.vlm_path,
                '--device', 'cuda:0',
                '--with-persons',
                '--draw'  # 시각화 결과 생성
            ]
            
            logger.info(f"memi 명령어: {' '.join(cmd)}")
            
            # memi 실행 (subprocess)
            start_time = time.time()
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                cwd=str(self.memi_path)
            )
            # 실시간 로그 출력
            output_lines = []
            for line in process.stdout:
                logger.info(f"[memi] {line.rstrip()}")
                output_lines.append(line)
            
            # 프로세스 완료 대기
            return_code = process.wait()
            processing_time = time.time() - start_time
            
            if return_code != 0:
                error_msg = f"memi 실행 실패 (exit code: {return_code})"
                logger.error(f"{error_msg}")
                logger.error(f"Output:\n{''.join(output_lines[-50:])}")  # 마지막 50줄만
                raise RuntimeError(error_msg)
            
            logger.info(f"memi 분석 완료: {processing_time:.2f}초")
            
            # 결과 파일 파싱
            result = self._parse_memi_results(output_dir)
            result['processing_time'] = processing_time
            result['output_dir'] = str(output_dir)
            
            return result
            
        except Exception as e:
            logger.error(f"memi 실행 오류: {type(e).__name__}: {str(e)}")
            raise
        
    def _parse_memi_results(self, output_dir: Path) -> Dict[str, Any]:
        """
        memi 출력 결과 파싱
        
        Args:
            output_dir: memi 출력 디렉토리
        
        Returns:
            파싱된 결과 딕셔너리
        """
        try:
            logger.info(f"memi 결과 파싱: {output_dir}")
            
            result = {
                'status': 'completed',
                'output_files': [],
                'analysis_summary': {}
            }
            
            # 출력 파일 목록
            output_files = list(output_dir.glob('**/*'))
            result['output_files'] = [str(f.relative_to(output_dir)) for f in output_files if f.is_file()]
            
            logger.info(f"   출력 파일 수: {len(result['output_files'])}")
            
            # JSON 결과 파일 찾기 (memi가 생성하는 경우)
            json_files = list(output_dir.glob('*.json'))
            if json_files:
                with open(json_files[0], 'r') as f:
                    analysis_data = json.load(f)
                    result['analysis_summary'] = analysis_data
                    logger.info(f"   JSON 결과 로드: {json_files[0].name}")
            
            # 비디오 파일 찾기 (시각화 결과)
            video_files = list(output_dir.glob('*.mp4')) + list(output_dir.glob('*.avi'))
            if video_files:
                result['annotated_video'] = str(video_files[0])
                logger.info(f"   시각화 비디오: {video_files[0].name}")
            
            return result
            
        except Exception as e:
            logger.error(f"결과 파싱 실패: {e}")
            return {
                'status': 'completed',
                'output_files': [],
                'analysis_summary': {},
                'error': str(e)
            }
    
    def _save_processing_result(self, video_id: str, result: Dict[str, Any]) -> bool:
        """
        처리 결과를 Django DB + PostgreSQL에 저장
    
        Args:
            video_id: 비디오 ID
            result: memi 분석 결과
    
        Returns:
            저장 성공 여부
        """
        try:
            logger.info(f"DB 저장 시작: video_id={video_id}")
            
            # Django ORM으로 Video 업데이트
            video = Video.objects.get(video_id=video_id)
            
            # major_event 필드에 분석 결과 저장 (JSONField)
            video.major_event = result.get('analysis_summary', {})
            
            # processing_status 업데이트
            if hasattr(video, 'processing_status'):
                video.processing_status = 'completed'
            
            # analysis_status 업데이트
            if hasattr(video, 'analysis_status'):
                video.analysis_status = 'completed'
            
            # 처리 완료 시간
            if hasattr(video, 'analyzed_at'):
                video.analyzed_at = datetime.now(timezone.utc)
            
            video.save()
            
            logger.info(f"DB 저장 완료: video_id={video_id}")
            
            # 시각화 비디오가 있으면 S3에 업로드
            if 'annotated_video' in result:
                self._upload_annotated_video_to_s3(video_id, result['annotated_video'])
            
            return True
            
        except Video.DoesNotExist:
            logger.error(f"Video not found: video_id={video_id}")
            
            # video_id가 없으면 새로 생성 (옵션)
            logger.info(f"새로운 Video 레코드 생성 시도...")
            try:
                video = Video.objects.create(
                    video_id=video_id,
                    major_event=result.get('analysis_summary', {}),
                    processing_status='completed',
                    analysis_status='completed',
                    analyzed_at=datetime.now(timezone.utc)
                )
                logger.info(f"새 Video 생성 완료: video_id={video_id}")
                return True
            except Exception as create_error:
                logger.error(f"Video 생성 실패: {create_error}")
                return False
            
        except Exception as e:
            logger.error(f"DB 저장 실패: {e}")
            return False
    
    def _upload_annotated_video_to_s3(self, video_id: str, video_path: str):
        """시각화된 비디오를 S3에 업로드"""
        try:
            # S3 키 생성
            s3_key = f"processed/{datetime.now().year}/{datetime.now().month:02d}/{datetime.now().day:02d}/video_{video_id}_annotated.mp4"
            
            # S3 업로드
            s3_service.upload_file(video_path, s3_key)
            
            logger.info(f"시각화 비디오 업로드: s3://{s3_service.bucket_name}/{s3_key}")
            
            # Django DB에 processed_video_url 저장
            video = Video.objects.get(video_id=video_id)
            if hasattr(video, 's3_processed_key'):
                video.s3_processed_key = s3_key
                video.save()
            
        except Exception as e:
            logger.error(f"시각화 비디오 업로드 실패: {e}")
    
    def _parse_message_body(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        SQS 메시지 파싱 (S3 Event Notification 형식)
        
        Args:
            message: SQS 메시지
        
        Returns:
            파싱된 메시지 정보
        """
        try:
            body = json.loads(message['Body'])
            
            # S3 Event Notification 형식 파싱
            if 'Records' in body and len(body['Records']) > 0:
                record = body['Records'][0]
                
                # S3 이벤트인지 확인
                if record.get('eventSource') == 'aws:s3':
                    s3_info = record['s3']
                    
                    parsed = {
                        'bucket': s3_info['bucket']['name'],
                        'key': s3_info['object']['key'],
                        'size': s3_info['object'].get('size', 0),
                        'etag': s3_info['object'].get('eTag', ''),
                        'event_time': record.get('eventTime'),
                        'event_name': record.get('eventName')
                    }
                    
                    # S3 key에서 video_id 추출
                    video_id = self._get_video_id_from_django(parsed['key'])
                    
                    if video_id:
                        parsed['video_id'] = video_id
                        logger.info(f"video_id 조회 성공: {video_id}")
                    else:
                        logger.warning(f"video_id를 찾을 수 없음, 새로 생성 필요: {parsed['key']}")
                        parsed['video_id'] = None
                    
                    logger.info(f"S3 Event 파싱 완료: {parsed}")
                    return parsed
            
            raise ValueError("Unknown message format")
            
        except Exception as e:
            logger.error(f"메시지 파싱 실패: {e}")
            raise

    def _get_video_id_from_django(self, s3_key: str) -> Optional[str]:
        """
        S3 key로 Django API에서 video_id 조회
        
        Args:
            s3_key: S3 객체 키
        
        Returns:
            video_id
        """
        try:
            # Django API 엔드포인트
            django_api_url = os.environ.get('DJANGO_API_URL', 'http://backend:8000')
            
            response = requests.get(
                f"{django_api_url}/db/videos/by-s3-key/",
                params={'s3_key': s3_key},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                return str(data['video_id'])
            else:
                logger.warning(f"Django API 응답: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Django API 호출 실패: {e}")
            return None
    
    def _cleanup_temp_files(self, file_path: str):
        """임시 파일 정리"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"🗑️ 임시 파일 삭제: {file_path}")
        except Exception as e:
            logger.warning(f"⚠️ 임시 파일 삭제 실패: {e}")
    
def main():
    """메인 실행 함수"""
    try:
        worker = GPUVideoWorker()
        worker.start_worker_loop()
    except Exception as e:
        logger.error(f"❌ 워커 실행 실패: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()