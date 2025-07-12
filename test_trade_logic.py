import Monitoring as mon
from decimal import Decimal


def simulate(prices):
    state = mon.TradeState(
        buy_price_usd=prices[0],
        highest_price_seen=prices[0],
        remaining_qty=Decimal('1')
    )
    actions = []
    for price in prices[1:]:
        result = mon.process_price_update(state, price, 'TOKEN', 'ADDR')
        actions.append((price, state.remaining_qty, state.partial_tp_triggered))
        if result == 'closed':
            break
    return actions


def test_partial_then_full_tp():
    actions = simulate([1.0, 1.05, 1.04, 1.09])
    # Expect partial sell after 1.05 and full close at 1.09
    assert actions[0][1] == Decimal('0.5')  # remaining after partial
    assert actions[-1][1] == Decimal('0')


def test_partial_then_trailing_sl():
    actions = simulate([1.0, 1.06, 1.02])
    # After drop below trailing buffer, remaining qty should be zero
    assert actions[-1][1] == Decimal('0')
    assert actions[0][1] == Decimal('0.5')


def test_fixed_stop_loss():
    actions = simulate([1.0, 0.97])
    assert actions[-1][1] == Decimal('0')

