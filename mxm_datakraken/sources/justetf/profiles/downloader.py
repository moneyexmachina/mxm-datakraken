"""
Downloader for justETF profiles.

This module provides a low-level function to fetch the raw HTML
of an ETF profile page from justETF, given its ISIN and profile URL.
"""

from __future__ import annotations

import requests

USER_AGENT: str = "mxm-datakraken/0.1 (contact@moneyexmachina.com)"


def download_etf_profile_html(isin: str, url: str, timeout: int = 30) -> str:
    """
    Download the raw HTML of an ETF profile page from justETF.

    Args:
        isin: The ISIN of the ETF (used for logging/debugging only).
        url: The profile URL to download.
        timeout: Timeout in seconds for the HTTP request.

    Returns:
        The HTML content of the profile page as a string.

    Raises:
        requests.HTTPError: If the response has an unsuccessful status code.
        requests.RequestException: For network-related errors.
    """
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.text
