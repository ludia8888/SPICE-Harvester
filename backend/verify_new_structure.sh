#!/bin/bash
# 🔥 THINK ULTRA!! Verification Script for New Structure

echo "🔥 THINK ULTRA!! Verifying new spice_harvester structure..."
echo ""

# Check if spice_harvester package exists
echo "1. Checking spice_harvester package structure:"
if [ -d "spice_harvester" ]; then
    echo "✅ spice_harvester directory exists"
    
    # Check subdirectories
    REQUIRED_DIRS=("oms" "bff" "data_connector" "shared")
    for dir in "${REQUIRED_DIRS[@]}"; do
        if [ -d "spice_harvester/$dir" ]; then
            echo "✅ spice_harvester/$dir exists"
        else
            echo "❌ spice_harvester/$dir missing!"
        fi
    done
else
    echo "❌ spice_harvester directory not found!"
fi

echo ""
echo "2. Checking startup scripts:"
SCRIPTS=("start_oms.sh" "start_bff.sh")
for script in "${SCRIPTS[@]}"; do
    if [ -f "$script" ]; then
        if grep -q "spice_harvester" "$script"; then
            echo "✅ $script updated to use new structure"
        else
            echo "❌ $script still uses old structure!"
        fi
    else
        echo "❌ $script not found!"
    fi
done

echo ""
echo "3. Checking Docker configuration:"
if [ -f "docker-compose.yml" ]; then
    if grep -q "spice_harvester" "docker-compose.yml"; then
        echo "✅ docker-compose.yml updated to use new structure"
    else
        echo "❌ docker-compose.yml still uses old structure!"
    fi
else
    echo "❌ docker-compose.yml not found!"
fi

echo ""
echo "4. Testing Python imports:"
python -c "import spice_harvester" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✅ spice_harvester package can be imported"
else
    echo "❌ spice_harvester package import failed!"
fi

echo ""
echo "🔥 Verification complete!"
