from typing import Any, AsyncGenerator, Optional, Callable, Coroutine

from port_ocean.utils import http_async_client
import httpx
from loguru import logger

"""
HTTP based sync protocol:

Request: integration -> CloudFunction
```json
{
    "agent" : "<integration>/<version>",
    "state": { ... },
    "secrets": {
        "apiToken": "abcdefghijklmnopqrstuvwxyz_0123456789"
    }
}
```

*behind the scenes upstream API calls*

Response: CloudFunction -> integration
```json
{
    "state": { ... },
    "insert": [
            {"id":101, "name": "Christmas"},
            {"id":102, "name": "New Year"}
        ],
    "hasMore" : true
}
```
"""


class CloudFunctionClient:
    def __init__(
        self,
        agent: str,
        http_client: httpx.AsyncClient,
        token_supplier: Callable[[], Coroutine[Any, Any, Optional[str]]],
        function_url: str,
        secrets: dict[str, str],
    ) -> None:
        self.agent = agent
        self.http_client = http_client
        self.token_supplier = token_supplier
        self.function_url = function_url
        self.secrets = secrets

    async def sync(self, kind: str) -> AsyncGenerator[list[dict[str, Any]]]:
        has_more = True
        state = None
        while has_more:
            res_body = await self._fetch(url=self.function_url, kind=kind, state=state)
            state = res_body.get("state", None)
            yield res_body.get("insert", [])
            has_more = res_body.get("hasMore", False)

    async def _fetch(
        self, url: str, kind: str, state: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        try:
            json_data = {
                "agent": self.agent,
                "secrets": self.secrets,
                "kind": kind,
                "state": state,
            }

            token = await self.token_supplier()
            headers = {}
            if token:
                logger.debug("using OAuth ID token for Cloud Function request")
                headers = {"Authorization": f"Bearer {token}"}
            else:
                logger.debug("no ID token available, making unauthenticated request")

            response = await self.http_client.request(
                method="POST",
                url=self.function_url,
                json=json_data,
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(
                    f"Requested resource not found: {url}; message: {str(e)}"
                )
                return {}
            logger.error(f"API request failed for {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during API request to {url}: {e}")
            raise

def create_client(agent: str, token_supplier: Callable[[], Coroutine[Any, Any, Optional[str]]], function_url: str, secrets: dict[str, str]):
    return CloudFunctionClient(
        agent=agent,
        http_client=http_async_client,
        token_supplier=token_supplier,
        function_url=function_url,
        secrets=secrets,
    )
