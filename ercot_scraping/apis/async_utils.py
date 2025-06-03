import asyncio
import aiohttp
import logging
import traceback

logger = logging.getLogger("ercot_scraping.async_utils")


async def fetch_page(session, url, params, headers=None):
    page = params.get("page", "?")
    logger.info(
        f"START fetch page {page} | called from: {traceback.format_stack(limit=3)}")
    async with session.get(url, params=params, headers=headers) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info(
            f"END fetch page {page} (records: {len(data) if hasattr(data, '__len__') else 'unknown'}) | called from: {traceback.format_stack(limit=3)}")
        return data


async def fetch_all_pages(
        base_url,
        page_params_list,
        headers=None,
        max_concurrent=10):
    logger.info(
        f"Starting fetch_all_pages with {len(page_params_list)} pages and max_concurrent={max_concurrent} | called from: {traceback.format_stack(limit=3)}")
    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        async def sem_fetch(params):
            async with semaphore:
                return await fetch_page(session, base_url, params, headers)
        tasks = [sem_fetch(params) for params in page_params_list]
        results = await asyncio.gather(*tasks)
        logger.info("Completed fetch_all_pages")
        return results

# No changes needed: fetch_all_pages and fetch_page are already async and support parallelism via semaphore.
