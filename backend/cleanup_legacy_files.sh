#!/bin/bash

# 🔥 THINK ULTRA! Legacy Files Cleanup Script
# This script moves legacy test files and documentation to an archive folder

echo "🧹 SPICE HARVESTER Legacy Files Cleanup"
echo "========================================"

# Create archive directory with timestamp
ARCHIVE_DIR="legacy_archive_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$ARCHIVE_DIR"
mkdir -p "$ARCHIVE_DIR/test_scripts"
mkdir -p "$ARCHIVE_DIR/documentation"
mkdir -p "$ARCHIVE_DIR/fix_scripts"

echo "📁 Archive directory created: $ARCHIVE_DIR"

# Count files before cleanup
TOTAL_FILES=0

# Move test files from root
echo ""
echo "1️⃣ Moving test_*.py files from root..."
TEST_FILES=$(find . -maxdepth 1 -type f -name "test_*.py" | wc -l)
if [ "$TEST_FILES" -gt 0 ]; then
    find . -maxdepth 1 -type f -name "test_*.py" -exec mv {} "$ARCHIVE_DIR/test_scripts/" \;
    echo "   ✅ Moved $TEST_FILES test files"
    TOTAL_FILES=$((TOTAL_FILES + TEST_FILES))
fi

# Move FIX_ files
echo ""
echo "2️⃣ Moving FIX_*.py files..."
FIX_FILES=$(find . -maxdepth 1 -type f -name "FIX_*.py" | wc -l)
if [ "$FIX_FILES" -gt 0 ]; then
    find . -maxdepth 1 -type f -name "FIX_*.py" -exec mv {} "$ARCHIVE_DIR/fix_scripts/" \;
    echo "   ✅ Moved $FIX_FILES fix scripts"
    TOTAL_FILES=$((TOTAL_FILES + FIX_FILES))
fi

# Move legacy documentation
echo ""
echo "3️⃣ Moving legacy documentation..."
DOC_FILES=0
for pattern in "LEGACY_*.md" "CRITICAL_*.md" "CORRECTED_*.md" "ARCHITECTURE_*.md" "CQRS_*.md"; do
    COUNT=$(find . -maxdepth 1 -type f -name "$pattern" | wc -l)
    if [ "$COUNT" -gt 0 ]; then
        find . -maxdepth 1 -type f -name "$pattern" -exec mv {} "$ARCHIVE_DIR/documentation/" \;
        DOC_FILES=$((DOC_FILES + COUNT))
    fi
done
if [ "$DOC_FILES" -gt 0 ]; then
    echo "   ✅ Moved $DOC_FILES documentation files"
    TOTAL_FILES=$((TOTAL_FILES + DOC_FILES))
fi

# Keep important migration files
echo ""
echo "4️⃣ Preserving important files..."
PRESERVED_FILES=(
    "MIGRATION_PROGRESS.md"
    "MIGRATION_COMPLETE_SUMMARY.md"
    "PRODUCTION_MIGRATION_RUNBOOK.md"
    "TEST_CONSOLIDATION_PLAN.md"
)

for file in "${PRESERVED_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "   ✅ Keeping: $file"
    fi
done

# Generate cleanup report
echo ""
echo "5️⃣ Generating cleanup report..."
cat > "$ARCHIVE_DIR/CLEANUP_REPORT.md" << EOF
# Legacy Files Cleanup Report
Date: $(date)

## Summary
- Total files archived: $TOTAL_FILES
- Test scripts: $TEST_FILES
- Fix scripts: $FIX_FILES
- Documentation: $DOC_FILES

## Archive Structure
\`\`\`
$ARCHIVE_DIR/
├── test_scripts/      # Temporary test files
├── fix_scripts/       # Fix and debug scripts
├── documentation/     # Legacy documentation
└── CLEANUP_REPORT.md  # This report
\`\`\`

## Preserved Files
The following important files were kept in place:
$(for file in "${PRESERVED_FILES[@]}"; do echo "- $file"; done)

## Notes
These files were archived as part of the S3/MinIO Event Store migration cleanup.
They can be safely deleted after review if no longer needed.
EOF

echo "   ✅ Report generated: $ARCHIVE_DIR/CLEANUP_REPORT.md"

# Create a README in the root about the cleanup
echo ""
echo "6️⃣ Updating root documentation..."
cat >> CLEANUP_LOG.md << EOF

## Cleanup performed on $(date)
- Archived $TOTAL_FILES legacy files to $ARCHIVE_DIR
- Preserved migration documentation
- System is now clean and production-ready
EOF

# Summary
echo ""
echo "======================================"
echo "✅ CLEANUP COMPLETE!"
echo "======================================"
echo ""
echo "📊 Results:"
echo "   • Files archived: $TOTAL_FILES"
echo "   • Archive location: $ARCHIVE_DIR"
echo "   • Important files preserved: ${#PRESERVED_FILES[@]}"
echo ""
echo "💡 Next steps:"
echo "   1. Review archived files in $ARCHIVE_DIR"
echo "   2. Delete archive after 30 days if not needed"
echo "   3. Commit the cleaned repository"
echo ""
echo "🔥 THINK ULTRA! Your codebase is now clean!"