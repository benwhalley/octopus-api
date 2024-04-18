"""
A command line utility and python functions to run HTTP requests in bulk with retries and backoff

At minimum, accepts `job_list` (json list of dicts with {'url': ..., 'params': ... })
Makes a POST request to url, with params as payload.


python reflect.py &
cat jobs.json |  python octo.py -e -r 1000 -s 1
python octo.py -e -r 1000 -s 1 -j jobs.json -o done.json && cat done.json

"""

import sys
import json
import argparse
from octopus_api import OctopusApi


# Some sample jobs to make, posting to `python reflect.py`
# which simply echoes post requests

# requests_list = [{
#     "url": "http://127.0.0.1:8001",
#     "params": {"title": f"title number {i}" }} for i in range(10)
# ]

# with open('jobs.json', 'w') as f:
#     f.write(json.dumps(requests_list))


async def getpage(session, request):
    async with session.post(url=request["url"], data=json.dumps(request["payload"]), headers=request.get("headers", None)) as response:
        stat = response.status
        bd = await response.text()
        return {"status": stat, "body": bd }


def run_requests(args_dict):
    # when calling from R with reticulate or another python programme we might not have all args set
    default_args = get_default_args()
    default_args.update(args_dict)
    jbs = json.loads(default_args['job_list'])
    default_args['job_list']= [v for k,v in jbs.items()]
    # print("JOB LIST")
    # print( default_args['job_list'])
    # print("Running requests now")
    return run_requests_(default_args)


def run_requests_(args):
    # Load job requests
    # job_list is supplied as a json string
    # which is a list of dictionaries with url and payload as keys
    if args.get("job_list", None):
        requests_list = args.get("job_list")
    else:
        with open(args["jobs"], "r") as f:
            requests_list = json.loads(f.read())

    # Set up the API client
    client = OctopusApi(
        rate=args["rpm"] / 60,
        resolution="sec",
        retries=args["trys"],
        retry_sleep=args["sleep"],
        connections=args["connections"],
    )
    # Execute requests and get results
    result = client.execute(requests_list=requests_list, func=getpage)

    if args.get("output", None):
        # Save results to the specified output file
        with open(args["output"], "w") as f:
            json.dump(result, f, indent=2)

    if args.get("echo", None):
        print(json.dumps(result, indent=2))

    return result


def setup_parser():
    parser = argparse.ArgumentParser(
        description="Run HTTP requests from a JSON file or piped input and save results."
    )
    parser.add_argument(
        "-j", "--jobs", type=str, help="Optional path to JSON file with job requests"
    )
    parser.add_argument(
        "-e", "--echo", action="store_true", help="Echo results to stdout"
    )
    parser.add_argument(
        "-o", "--output", type=str, help="Path to save the results of the jobs"
    )
    parser.add_argument(
        "-r", "--rpm", type=int, default=200, help="Requests per minute to set rate"
    )
    parser.add_argument(
        "-t", "--trys", type=int, default=10, help="Number of retries for HTTP requests"
    )
    parser.add_argument(
        "-s",
        "--sleep",
        type=int,
        default=10,
        help="Number of seconds to wait for first retry (thereafter exponential)",
    )
    parser.add_argument(
        "-x",
        "--max-time",
        type=int,
        default=60 * 10,
        help="Max number of seconds to wait.",
    )
    parser.add_argument(
        "-c",
        "--connections",
        type=int,
        default=10,
        help="Number of connections to host",
    )

    return parser


def get_default_args():
    """Use the argparse parser to create a default dictionary"""
    parser = setup_parser()
    args = parser.parse_args([])
    return vars(args)


def main():
    parser = setup_parser()
    args = parser.parse_args()

    # Check if any data is being piped into stdin
    # input_json = sys.stdin.read()
    # if input_json:
    #     args.job_list = json.loads(input_json)
        # todo: could do some input parsing here

    # vars(.) converts to a dict for compatibility with the function called by R
    run_requests_(vars(args))


if __name__ == "__main__":
    main()
