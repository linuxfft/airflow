from airflow.models import BaseOperator
import pika
from plugins.utils.logger import generate_logger
from functools import wraps
from airflow.hooks.base_hook import BaseHook

_logger = generate_logger(__name__)


def with_channel(func):
    @wraps(func)
    def wrapper(
        conn_id=None, queue=None, queue_args={}, exchange=None, exchange_args={},
        binding_args={}, **kwargs
    ):
        connection = RabbitmqHook.get_connection(conn_id)
        channel: pika.adapters.blocking_connection.BlockingChannel = connection.channel()
        channel.confirm_delivery()
        if queue:
            channel.queue_declare(
                queue,
                **queue_args
            )
        if exchange:
            channel.exchange_declare(
                exchange=exchange,
                **exchange_args
            )
        if queue and exchange:
            channel.queue_bind(
                exchange=exchange,
                queue=queue,
                **binding_args
            )

        ret = func(
            conn_id=conn_id,
            channel=channel,
            queue=queue,
            queue_args=queue_args,
            exchange=exchange,
            exchange_args=exchange_args,
            binding_args=binding_args,
            **kwargs
        )

        connection.close()
        return ret

    return wrapper


class RabbitmqHook(BaseHook):

    @classmethod
    def get_connection(cls, conn_id: str):
        from airflow.models.connection import Connection
        mq = Connection.get_connection_from_secrets(conn_id)
        if mq is None:
            raise Exception(f'Connection {conn_id} not found')
        _logger.info('{}:{}, {},{}'.format(mq.host, mq.port, mq.login, mq.get_password()))
        credentials = pika.PlainCredentials(mq.login, mq.get_password())
        extra = mq.extra_dejson
        connection_config = {
            'host': mq.host,
            'port': mq.port,
            'credentials': credentials,
            'virtual_host': extra.get('vhost', '/')
        }
        return pika.BlockingConnection(
            pika.ConnectionParameters(**connection_config)
        )

    @staticmethod
    @with_channel
    def subscribe(
        queue=None,
        message_handler=None,
        subscribe_args=None,
        channel: pika.adapters.blocking_connection.BlockingChannel = None,
        **kwargs
    ):
        while True:
            try:
                channel.basic_consume(
                    queue,
                    on_message_callback=message_handler,
                    auto_ack=subscribe_args.get('auto_ack', True)
                )
                channel.start_consuming()
            except Exception as e:
                _logger.error(e)
                channel.stop_consuming()

    @staticmethod
    @with_channel
    def publish(
        message_source=None,
        exchange=None,
        routing_key='*',
        publish_args={},
        channel: pika.adapters.blocking_connection.BlockingChannel = None,
        **kwargs
    ):
        if hasattr(message_source, '__call__'):
            for msg in message_source():
                _logger.debug(f'publishing: {msg}')
                channel.basic_publish(exchange=exchange, routing_key=routing_key, body=msg, **publish_args)
            return
        _logger.debug(f'publishing: {message_source}')
        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message_source, **publish_args)
        _logger.debug('published')


class RabbitmqOperator(BaseOperator):
    connection = None

    def __init__(
        self,
        mq_config={},
        *args,
        **kwargs
    ):
        conn_id = mq_config.get('conn_id', None)
        queue = mq_config.get('queue', None)
        queue_args = mq_config.get('queue_args', None)
        exchange = mq_config.get('exchange', None)
        exchange_args = mq_config.get('exchange_args', None)
        binding_args = mq_config.get('binding_args', None)
        message_handler = mq_config.get('message_handler', None)
        message_source = mq_config.get('message_source', None)
        publish_args = mq_config.get('publish_args', None)
        subscribe_args = mq_config.get('subscribe_args', None)
        if not conn_id:
            raise Exception('RabbitmqOperator must have a connection_id')
        super().__init__(*args, **kwargs)
        self.subscribe_args = subscribe_args if subscribe_args is not None else {}
        self.publish_args = publish_args if publish_args is not None else {}
        self.binding_args = binding_args if binding_args is not None else {}
        self.exchange_args = exchange_args if exchange_args is not None else {}
        self.mq_queue_args = queue_args if queue_args is not None else {}
        self.exchange = exchange
        self.mq_queue = queue

        self.conn_id = conn_id
        self.message_handler = message_handler
        self.message_source = message_source

    def execute(self, context):
        if self.message_handler is not None:
            _logger.info('message_handler found, run in subscribe mode')
            RabbitmqHook.subscribe(
                conn_id=self.conn_id,
                queue=self.mq_queue,
                queue_args=self.mq_queue_args,
                exchange=self.exchange,
                exchange_args=self.exchange_args,
                message_handler=self.message_handler,
                subscribe_args=self.subscribe_args
            )

        if self.message_source is not None:
            _logger.info('message_source found, run in publish mode')
            RabbitmqHook.publish(
                conn_id=self.conn_id,
                message_source=self.message_source,
                exchange=self.exchange,
                publish_args=self.publish_args
            )
