scriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
conda activate operations

python $scriptDir/run.py --job=$1
