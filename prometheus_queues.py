import json
import logging
import sys
import urllib.request

logging.basicConfig(
    level=logging.DEBUG,
    filename='prometheus_queues.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filemode='w'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


prometheus_url = ('http://prometheusurl:9090/'
                  'api/v1/query?query='
                  'nifi_amount_items_queued'
                  '>=nifi_backpressure_object_threshold')


def get_data(url):
    try:
        response = urllib.request.urlopen(url)
        json_response = json.loads(response.read())
        logger.info(f'Получили список метрик из prometheus: {json_response}')
    except Exception as e:
        logger.error(f'Недоступен эндпоинт: {e}')
    try:
        return json_response['data'].get('result')
    except Exception as e:
        logger.error(f'Не удалось вернуть json: {e}')


def parse_data(promjson):
    for metric in promjson:
        metric_with_value = {
            "metric_name": metric.get("metric").get("__name__"),
            "metric_value": metric.get("value")[1],
            "instance": metric.get("metric").get("instance"),
            "component_name": metric.get("metric").get("component_name"),
            "parent_id": metric.get("metric").get("parent_id"),
            "url_to_error":
                f'http://sys-nifi-1.sys.local/nifi/?processGroupId='
                f'{metric.get("metric").get("parent_id")}'
                f'&componentIds={metric.get("metric").get("component_id")}'

        }
        print(json.dumps(metric_with_value))


def main():
    logger.info("Скрипт запущен")

    prometheus_data = get_data(prometheus_url)
    if not prometheus_data:
        message = 'Ответ от prometheus не вернулся или пустой'
        print(message)
        logger.error(message)
        sys.exit()
    parse_data(prometheus_data)
    sys.exit()


if __name__ == '__main__':
    main()
