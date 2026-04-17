#!/bin/bash
# Preview the AhanaFlow competitive benchmark website
# Opens in default browser

cd "$(dirname "$0")"
WEBSITE_DIR="/media/jeremiah/Internal SSD 24/Software Projects/New Ahana Tool/business_ecosystem/33_event_streams/deploy_to_github/website"

echo "🌺 AhanaFlow Website Preview"
echo "=============================="
echo ""
echo "Opening website in browser..."
echo "Location: $WEBSITE_DIR/index.html"
echo ""

# Open in default browser
xdg-open "$WEBSITE_DIR/index.html" 2>/dev/null || \
  gnome-open "$WEBSITE_DIR/index.html" 2>/dev/null || \
  firefox "$WEBSITE_DIR/index.html" 2>/dev/null || \
  google-chrome "$WEBSITE_DIR/index.html" 2>/dev/null || \
  echo "Please open $WEBSITE_DIR/index.html manually in your browser"

echo ""
echo "Website highlights:"
echo "  ✅ 47.6k req/s mixed load in compact fast-mode"
echo "  ✅ 1.157× Redis on the official pipelined KV lane"
echo "  ✅ 3.42× smaller WAL than Redis AOF"
echo "  ✅ RESP-compatible async lane with honest boundaries"
echo "  ✅ Exact + HNSW vector operations for controlled deployment"
echo ""
echo "Deployment ready:"
echo "  📦 Docker: cd business_ecosystem/33_event_streams && docker-compose up"
echo "  🐍 Python: python -m universal_server.cli serve --port 9633"
echo "  📊 Benchmarks: python benchmark_vs_competitors.py"
echo ""
