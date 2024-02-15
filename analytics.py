import http
import pandas as pd
from decimal import Decimal
import re


with open("visit_latimes.csv", "r", encoding="UTF-8") as f:
    responses = pd.read_csv(f, header=0)
    total_urls_extracted = responses["outlink"].sum()
    L1KB = responses[responses["size"] < 1024].shape[0]
    L10KB = responses[(1024 <= responses["size"]) & (responses["size"] < 10 * 1024)].shape[0]
    L100KB = responses[(10 * 1024 <= responses["size"]) & (responses["size"] < 100 * 1024)].shape[0]
    L1MB = responses[(100 * 1024 <= responses["size"]) & (responses["size"] < 1024 * 1024)].shape[0]
    G1MB = responses[1024 * 1024 <= responses["size"]].shape[0]
    contentTypes = responses.groupby(responses["Content-Type"]).count().to_dict()["url"]

with open("urls_latimes.csv", "r", encoding="UTF-8") as f:
    responses = pd.read_csv(f, header=0)
    uniqueExtracted = responses.shape[0]
    uniqueWithin = responses[responses["domain"] == "OK"].shape[0]
    uniqueOutside = responses[responses["domain"] == "N_OK"].shape[0]

with open("fetch_latimes.csv", "r", encoding="UTF-8") as f:
    responses = pd.read_csv(f, header=0)
    fetchesAttempted = responses.shape[0]
    fetchesSucceeded = responses[responses["status"] < 300].shape[0]
    fetchesFailed = responses[responses["status"] > 300].shape[0]
    statusCodes = responses.groupby(responses["status"]).count().to_dict()["url"]

with open("crawlReport_latimes.txt", "w") as f:
    
    f.write(f"Name: Meivenkatkumar Lakshminarayanan\n")
    f.write(f"USC ID: 2638078100\n")
    f.write(f"News site crawled: latimes.com\n")
    f.write(f"Number of threads used for Scrapy: 16\n")
    f.write(f"\n")

    f.write(f"Fetch Statistics\n")
    f.write(f"================\n")
    f.write(f"fetches attempted: {fetchesAttempted}\n")
    f.write(f"fetches succeeded: {fetchesSucceeded}\n")
    f.write(f"fetches failed or aborted: {fetchesFailed}\n")
    f.write(f"\n")

    f.write(f"Outgoing URLs:\n")
    f.write(f"==============\n")
    f.write(f"Total URLs extracted: {total_urls_extracted}\n")
    f.write(f"# unique URLs extracted: {uniqueExtracted}\n")
    f.write(f"# unique URLs within News Site: {uniqueWithin}\n")
    f.write(f"# unique URLs outside News Site: {uniqueOutside}\n")
    f.write(f"\n")

    f.write(f"Status Codes:\n")
    f.write(f"=============\n")
    for statusCode in sorted(statusCodes.keys()):
        status = str(statusCode)
        status = status.rstrip('0').rstrip('.') if '.' in status else status
        f.write(f"{status} {http.HTTPStatus(statusCode).phrase}: {statusCodes[statusCode]}\n")
    f.write(f"\n")

    f.write(f"File Sizes:\n")
    f.write(f"===========\n")
    f.write(f"< 1KB: {L1KB}\n")
    f.write(f"1KB ~10KB: {L10KB}\n")
    f.write(f"10KB ~ <100KB: {L100KB}\n")
    f.write(f"100KB ~ <1MB: {L1MB}\n")
    f.write(f">= 1MB: {G1MB}\n")
    f.write(f"\n")

    f.write(f"Content Types:\n")
    f.write(f"==============\n")
    htmlcount = 0
    for content in sorted(contentTypes.keys()):
        if re.search('text/html', content):
            htmlcount+= contentTypes[content]
        else:
            f.write(f"{content}: {contentTypes[content]}\n")
    f.write(f"text/html: {htmlcount}\n")