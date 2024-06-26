import asyncio
import time
from typing import List, Dict, Any
import backoff

import aiohttp
from tqdm import tqdm


class TentacleSession(aiohttp.ClientSession):
    """TentacleSession is a wrapper around the aiohttp.ClientSession, where it introduces the retry and rate functionality
    missing in the default aiohttp.ClientSession.

    Args:
        sleep (float): The time the client will sleep after each request. \n
        retries (int): The number of retries for a successful request. \n
        retry_sleep (float): The time service sleeps between nonsuccessful request calls. Defaults to 1.0.

    Returns:
        TentacleSession(aiohttp.ClientSession)
    """

    retries: int

    def __init__(self, retries=3, retry_sleep=1.0, **kwargs):
        self.retries = retries
        self.retry_sleep = retry_sleep
        super().__init__(raise_for_status=True, **kwargs)


#     def __retry__(self, func, **kwargs) -> Any:
#         return func(**kwargs)

#     def get(self, **kwargs) -> Any:
#         return self.__retry__(func=super().get, **kwargs)

#     def patch(self, **kwargs) -> Any:
#         return self.__retry__(func=super().patch, **kwargs)

#     def post(self, **kwargs) -> Any:
#         return self.__retry__(func=super().post, **kwargs)

#     def put(self, **kwargs) -> Any:
#         return self.__retry__(func=super().put, **kwargs)

#     def request(self, **kwargs) -> Any:
#         return self.__retry__(func=super().request, **kwargs)


def backoff_hdlr(details):
    print(
        f"Backing off {details['wait']} seconds after {details['tries']} tries \n"
        f"{details}\n---\n"
    )


def custom_fibo(initial=3):
    a, b = (
        initial,
        initial + 1,
    )  # start the sequence with the initial value and initial+1
    while True:
        yield a
        # note we don't rise quite as steeply as regular fibo sequence
        a, b = b, a + b*.75


class OctopusApi:
    """Initiates the Octopus client.
    Args:
        rate (Optional[float]): The rate limits of the endpoint; default to no limit. \n
            resolution (Optional[str]): The time resolution of the rate (sec, minute), defaults to None.
            connections (Optional[int]): Maximum connections on the given endpoint, defaults to 5.

    Returns:
        OctopusApi
    """

    rate_sec: float = None
    connections: int
    retries: int
    retry_sleep: int
    max_time: int

    def __init__(
        self,
        rate: int = None,
        resolution: str = None,
        connections: int = 5,
        retries: int = 3,
        retry_sleep: int = 10,
        max_time: int = 60 * 10,
    ):

        if rate or resolution:
            if resolution.lower() not in ["minute", "sec"]:
                raise ValueError(
                    "Incorrect value of resolution, expecting minute or sec!"
                )
            if not rate:
                raise ValueError("Can not set resolution of rate without rate")
            self.rate_sec = rate / (60 if resolution.lower() == "minute" else 1)

        self.connections = connections
        self.retries = retries
        self.retry_sleep = retry_sleep
        self.max_time = max_time

    def get_coroutine(self, requests_list: List[Dict[str, Any]], func: callable):

        async def __tentacles__(
            rate: float,
            retries: int,
            retry_sleep: int,
            max_time: int,
            connections: int,
            requests_list: List[Dict[str, Any]],
            func: callable,
        ) -> List[Any]:

            # print(rate, retries, max_time, connections)
            responses_order: Dict = {}
            progress_bar = tqdm(total=len(requests_list))
            sleep = 1 / rate if rate else 0

            @backoff.on_exception(
                lambda: custom_fibo(initial=retry_sleep),
                aiohttp.ClientError,
                max_tries=retries,
                on_backoff=backoff_hdlr,
                max_time=max_time,
                jitter=None,
            )
            async def func_mod(session: aiohttp.ClientSession, request: Dict, itr: int):
                resp = await func(session, request)
                responses_order[itr] = resp
                progress_bar.update()

            conn = aiohttp.TCPConnector(limit_per_host=connections)
            async with TentacleSession(connector=conn) as session:

                tasks = set()
                itr = 0
                for request in requests_list:
                    if len(tasks) >= self.connections:
                        _done, tasks = await asyncio.wait(
                            tasks, return_when=asyncio.FIRST_COMPLETED
                        )
                    tasks.add(asyncio.create_task(func_mod(session, request, itr)))
                    await asyncio.sleep(sleep)
                    itr += 1
                await asyncio.wait(tasks)
                return [value for (key, value) in sorted(responses_order.items())]

        return __tentacles__(
            self.rate_sec,
            self.retries,
            self.retry_sleep,
            self.max_time,
            self.connections,
            requests_list,
            func,
        )

    def execute(self, requests_list: List[Dict[str, Any]], func: callable) -> List[Any]:
        """Execute the requests given the functions instruction.

        Empower asyncio libraries for performing parallel executions of the user-defined function.
        Given a list of requests, the result is ordered list of what the user-defined function returns.

        Args:
            requests_list (List[Dict[str, Any]): The list of requests in a dictionary format, e.g.
            [{"url": "http://example.com", "data": {...}, "headers": {...}}..]
            func (callable): The user-defined function to execute, this function takes in the following arguments.
                Args:
                    session (TentacleSession): The Octopus wrapper around the aiohttp.ClientSession.
                    request (Dict): The request within the requests_list above.

        Returns:
            List(func->return)
        """

        result = asyncio.run(self.get_coroutine(requests_list, func))
        if result:
            return result
        return []
