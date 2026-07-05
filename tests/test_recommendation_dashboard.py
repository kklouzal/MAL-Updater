from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mal_updater.recommendation_dashboard import render_recommendation_dashboard, write_recommendation_dashboard
from mal_updater.recommendations import Recommendation


class RecommendationDashboardTests(unittest.TestCase):
    def test_render_includes_sortable_requested_columns_and_escapes_content(self) -> None:
        item = Recommendation(
            kind="discovery_candidate",
            priority=87,
            provider_series_id="cr-1",
            title="A <Great> Show",
            season_title="A <Great> Show (English Dub)",
            provider="crunchyroll",
            reasons=["recommended by 2 watched/mapped seed title(s)", "MAL mean score: 8.2"],
            context={
                "available_via_providers": ["crunchyroll", "hidive"],
                "supporting_source_count": 2,
                "aggregated_recommendation_votes": 34,
                "mean": 8.2,
                "popularity": 321,
            },
        )

        html = render_recommendation_dashboard([item])

        for label in (
            "Title",
            "Score",
            "Source count",
            "Total votes",
            "Crunchyroll",
            "HIDIVE",
            "English dub",
            "MAL mean",
            "MAL popularity",
            "Reasons",
        ):
            self.assertIn(label, html)
        self.assertIn('data-key="score" data-type="number"', html)
        self.assertIn("A &lt;Great&gt; Show (English Dub)", html)
        self.assertIn("recommended by 2 watched/mapped seed title(s); MAL mean score: 8.2", html)
        self.assertIn("34", html)
        self.assertIn("321", html)
        self.assertIn("addEventListener('click'", html)

    def test_write_dashboard_creates_parent_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "nested" / "recommendations.html"
            written = write_recommendation_dashboard(output, [])

            self.assertEqual(output, written)
            html = output.read_text(encoding="utf-8")
            self.assertIn("No recommendations found.", html)
            self.assertIn("Click any column header to sort", html)


if __name__ == "__main__":
    unittest.main()
