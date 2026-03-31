"""Local service layer package for BeerMarketMiniAppBot.

Adding this file ensures Python imports the project's `services` package
instead of any third-party package with the same top-level name.
"""

from .media_service import MediaService
from .news_service import NewsService
from .permissions_service import extend_access_matrix

__all__ = ["MediaService", "NewsService", "extend_access_matrix"]
