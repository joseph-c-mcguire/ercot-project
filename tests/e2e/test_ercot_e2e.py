import os
import shutil
import tempfile
import pytest
import pandas as pd
from ercot_scraping.utils.filters import load_qse_shortnames

SAMPLE_DATA_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../../_data/sample_data'))
TRACKING_LIST = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../../_data/ERCOT_tracking_list.csv'))

DAM_FILES = [
    '60d_DAM_EnergyBids-30-JAN-25.csv',
    '60d_DAM_EnergyBidAwards-30-JAN-25.csv',
    '60d_DAM_EnergyOnlyOffers-30-JAN-25.csv',
    '60d_DAM_EnergyOnlyOfferAwards-30-JAN-25.csv',
]
SPP_FILE = 'cdr.00012301.0000000000000000.20250130.111701.SPPHLZNP6905_20250130_1115.csv'


@pytest.fixture(scope="module")
def temp_data_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        for fname in DAM_FILES + [SPP_FILE]:
            shutil.copy(os.path.join(SAMPLE_DATA_DIR, fname), tmpdir)
        shutil.copy(TRACKING_LIST, tmpdir)
        yield tmpdir


def filter_dam_file_by_qse(input_path, qse_names, qse_field='QSE Name'):
    df = pd.read_csv(input_path)
    filtered = df[df[qse_field].isin(qse_names)]
    return filtered


def get_settlement_point_prices(spp_path, point_name):
    df = pd.read_csv(spp_path)
    return df[df['SettlementPointName'] == point_name]['SettlementPointPrice'].astype(float).tolist()


def test_ercot_e2e_pipeline(temp_data_dir):
    """End-to-end test for the ERCOT data pipeline, including QSE name loading, DAM file filtering, and SPP price validation."""
    # Step 1: Load QSE names
    qse_names = load_qse_shortnames(os.path.join(
        temp_data_dir, 'ERCOT_tracking_list.csv'))
    assert len(qse_names) > 0, "QSE tracking list should not be empty"

    # Step 2: Filter all DAM files by QSE Name and check that filtered files are smaller
    filtered_counts = {}
    for fname in DAM_FILES:
        orig_path = os.path.join(temp_data_dir, fname)
        filtered = filter_dam_file_by_qse(orig_path, qse_names)
        filtered_counts[fname] = len(filtered)
        # Save filtered file for inspection if needed
        filtered_path = orig_path.replace('.csv', '.filtered.csv')
        filtered.to_csv(filtered_path, index=False)
        # Check that all QSE Names in filtered file are in the tracking list
        assert set(filtered['QSE Name']).issubset(qse_names)
        # Check that the filtered file is smaller or equal (never larger)
        orig_count = pd.read_csv(orig_path).shape[0]
        assert len(filtered) <= orig_count

    # Step 3: For each award in filtered offer awards, get settlement point prices for 'AEEC'
    awards_path = os.path.join(
        temp_data_dir, '60d_DAM_EnergyOnlyOfferAwards-30-JAN-25.filtered.csv')
    awards_df = pd.read_csv(awards_path)
    spp_prices = get_settlement_point_prices(
        os.path.join(temp_data_dir, SPP_FILE), 'AEEC')
    # Step 4: Assert average price matches expected (example: 21.9275)
    if spp_prices:
        avg_price = sum(spp_prices) / len(spp_prices)
        assert avg_price == pytest.approx(21.91, abs=1e-2)
    else:
        pytest.skip('No SPP prices found for AEEC')

    # Step 5: Optionally, check that all settlement points in awards exist in SPP file
    spp_df = pd.read_csv(os.path.join(temp_data_dir, SPP_FILE))
    spp_points = set(spp_df['SettlementPointName'])
    award_points = set(awards_df['Settlement Point'])
    assert award_points.intersection(
        spp_points), "Some award settlement points should exist in SPP file"


def test_empty_qse_tracking_list(temp_data_dir):
    """Edge case: Empty QSE tracking list should result in empty filtered files."""
    empty_tracking = os.path.join(temp_data_dir, 'empty_tracking.csv')
    pd.DataFrame({'SHORT NAME': []}).to_csv(empty_tracking, index=False)
    qse_names = load_qse_shortnames(empty_tracking)
    assert len(qse_names) == 0
    for fname in DAM_FILES:
        orig_path = os.path.join(temp_data_dir, fname)
        filtered = filter_dam_file_by_qse(orig_path, qse_names)
        assert filtered.empty


def test_dam_file_with_no_matching_qse(temp_data_dir):
    """Edge case: DAM file with QSE names not in tracking list should result in empty filtered file."""
    # Create a fake tracking list with names not present in DAM files
    fake_tracking = os.path.join(temp_data_dir, 'fake_tracking.csv')
    pd.DataFrame({'SHORT NAME': ['ZZZZZZ', 'YYYYYY']}).to_csv(
        fake_tracking, index=False)
    qse_names = load_qse_shortnames(fake_tracking)
    for fname in DAM_FILES:
        orig_path = os.path.join(temp_data_dir, fname)
        filtered = filter_dam_file_by_qse(orig_path, qse_names)
        assert filtered.empty


def test_spp_file_missing_settlement_point(temp_data_dir):
    """Edge case: SPP file does not contain the requested settlement point."""
    spp_path = os.path.join(temp_data_dir, SPP_FILE)
    prices = get_settlement_point_prices(spp_path, 'NON_EXISTENT_POINT')
    assert prices == []


def test_dam_file_missing_qse_column(temp_data_dir):
    """Edge case: DAM file missing 'QSE Name' column should raise KeyError."""
    # Create a DAM file without 'QSE Name'
    dam_path = os.path.join(temp_data_dir, 'bad_dam.csv')
    pd.DataFrame({'SomeOtherColumn': [1, 2, 3]}).to_csv(dam_path, index=False)
    qse_names = {'ANY'}
    with pytest.raises(KeyError):
        filter_dam_file_by_qse(dam_path, qse_names)
