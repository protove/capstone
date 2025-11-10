"""
EC2 GPU 인스턴스 자동 시작 Lambda 함수
SQS 메시지가 들어오면 GPU 인스턴스를 시작합니다.
"""
import boto3
import json
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ec2 = boto3.client('ec2')
sqs = boto3.client('sqs')

GPU_INSTANCE_ID = os.environ['GPU_INSTANCE_ID']
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')


def lambda_handler(event, context):
    """
    SQS 트리거로 호출되는 메인 핸들러
    
    Args:
        event: SQS 이벤트 (Records 배열)
        context: Lambda 컨텍스트
    
    Returns:
        처리 결과
    """
    logger.info(f"Lambda 시작: environment={ENVIRONMENT}")
    logger.info(f"SQS 메시지 수신: {len(event.get('Records', []))}개")
    
    try:
        # SQS 큐의 대기 중인 메시지 수 확인
        queue_attrs = sqs.get_queue_attributes(
            QueueUrl=SQS_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        message_count = int(queue_attrs['Attributes']['ApproximateNumberOfMessages'])
        logger.info(f"SQS 큐 대기 메시지: {message_count}개")
        
        if message_count > 0:
            # EC2 인스턴스 상태 확인
            instance_response = ec2.describe_instances(
                InstanceIds=[GPU_INSTANCE_ID]
            )
            
            if not instance_response['Reservations']:
                logger.error(f"인스턴스를 찾을 수 없음: {GPU_INSTANCE_ID}")
                return {
                    'statusCode': 404,
                    'body': json.dumps({'error': 'Instance not found'})
                }
            
            instance = instance_response['Reservations'][0]['Instances'][0]
            state = instance['State']['Name']
            
            logger.info(f"EC2 인스턴스 상태: {state}")
            
            # 인스턴스가 중지 상태이면 시작
            if state in ['stopped', 'stopping']:
                logger.info(f"▶EC2 인스턴스 시작 중: {GPU_INSTANCE_ID}")
                
                ec2.start_instances(InstanceIds=[GPU_INSTANCE_ID])
                
                logger.info(f"EC2 인스턴스 시작 완료: {GPU_INSTANCE_ID}")
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'EC2 GPU 인스턴스 시작 완료',
                        'instance_id': GPU_INSTANCE_ID,
                        'previous_state': state,
                        'queue_messages': message_count
                    })
                }
            
            elif state == 'running':
                logger.info(f"인스턴스 이미 실행 중: {GPU_INSTANCE_ID}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': '인스턴스 이미 실행 중',
                        'instance_id': GPU_INSTANCE_ID,
                        'state': state
                    })
                }
            
            elif state == 'pending':
                logger.info(f"인스턴스 시작 중: {GPU_INSTANCE_ID}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': '인스턴스 시작 중',
                        'instance_id': GPU_INSTANCE_ID,
                        'state': state
                    })
                }
            
            else:
                logger.warning(f"예상치 못한 인스턴스 상태: {state}")
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': f'Unexpected instance state: {state}',
                        'instance_id': GPU_INSTANCE_ID
                    })
                }
        
        else:
            logger.info("📭 SQS 큐가 비어있음 - 인스턴스 시작 불필요")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No messages in queue'})
            }
    
    except Exception as e:
        logger.error(f"Lambda 실행 오류: {type(e).__name__}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'type': type(e).__name__
            })
        }
