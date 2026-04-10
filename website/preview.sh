#!/bin/bash
# Preview the AhanaFlow competitive benchmark website
# Opens in default browser

cd "$(dirname "$0")"
WEBSITE_DIR="/media/jeremiah/Internal SSD 24/Software Projects/New Ahana Tool/business_ecosystem/33_event_streams/website"

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
echo "  ✅ 88.7% storage reduction vs Redis"
echo "  ✅ 0.26ms vector search (10× faster than pgvector)"
echo "  ✅ 5,010 vectors/sec insertion"
echo "  ✅ 95% recall@10 accuracy"
echo "  ✅ Competitive benchmarks vs Redis + pgvector/Qdrant"
echo ""
echo "Deployment ready:"
echo "  📦 Docker: cd business_ecosystem/33_event_streams && docker-compose up"
echo "  🐍 Python: python -m universal_server.cli serve --port 9633"
echo "  📊 Benchmarks: python benchmark_vs_competitors.py"
echo ""
