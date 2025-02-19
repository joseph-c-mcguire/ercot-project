# Get ERCOT Data

This tool allows you to fetch historical and current ERCOT market data including Day Ahead Market (DAM) energy bids/offers and Settlement Point Prices (SPP).

## Getting Started

1. Install dependencies:
```bash
pip install .
```

2. Create a secret file called `.env` and put these three passwords in it:
```env
ERCOT_API_USERNAME=your_username
ERCOT_API_PASSWORD=your_password 
ERCOT_API_SUBSCRIPTION_KEY=your_subscription_key
```

## How to Use It

You can do three fun things with this tool:

### 1. Get Market Data 🏪

This gets information about who wants to buy and sell electricity:

```bash
python main.py historical-dam --start 2024-01-01 --end 2024-01-02
```

### 2. Get Price Data 💰

This shows how much electricity costs:

```bash
python main.py historical-spp --start 2024-01-01 --end 2024-01-02
```

### 3. Put Everything Together 🧩

This combines all the information into one place:

```bash
python main.py merge-data
```

## What You Get

The tool will create a special box (database) that holds:
- How much electricity costs
- Who wants to buy electricity
- Who wants to sell electricity
- Who got to buy electricity
- Who got to sell electricity

## Need Help?

If you want to see more details about what's happening, add `--debug` to any command:

```bash
python main.py historical-dam --start 2024-01-01 --end 2024-01-02 --debug
```

## Secret Passwords You Need

You need three special passwords to make this work:

1. Username (like your name for a game)
2. Password (like a secret word to get in)
3. Special Key (like a magic key to open the door)

Ask your friendly ERCOT helper for these passwords! 🗝️
