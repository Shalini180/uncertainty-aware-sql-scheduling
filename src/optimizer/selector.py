# src/optimizer/selector.py
from enum import Enum
from typing import Dict, Optional
from dataclasses import dataclass
import logging

from src.core.compiler import ExecutionStrategy, QueryVariant
from src.optimizer.carbon_api import CarbonAPI, CarbonIntensity

logger = logging.getLogger(__name__)


class QueryUrgency(Enum):
    """Query urgency levels"""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    BATCH = "batch"

    def to_int(self) -> int:
        return {
            "critical": 5,
            "high": 4,
            "medium": 3,
            "low": 2,
            "batch": 1,
        }[self.value]


@dataclass
class SelectionContext:
    """Context for variant selection"""

    query: str
    urgency: QueryUrgency
    carbon_intensity: CarbonIntensity
    available_variants: Dict[ExecutionStrategy, QueryVariant]


@dataclass
class SelectionDecision:
    """Decision about which variant to use"""

    selected_strategy: ExecutionStrategy
    selected_variant: QueryVariant
    reason: str
    should_defer: bool = False
    defer_minutes: int = 0
    expected_energy: Optional[float] = None
    expected_carbon: Optional[float] = None

    def explain(self) -> str:
        """Human‑readable explanation"""
        explanation = [
            f"Selected: {self.selected_strategy.value.upper()}",
            f"Reason: {self.reason}",
        ]
        if self.should_defer:
            explanation.append(f"Deferring: {self.defer_minutes} minutes")
        if self.expected_energy:
            explanation.append(f"Expected energy: {self.expected_energy:.2f}J")
        if self.expected_carbon:
            explanation.append(f"Expected carbon: {self.expected_carbon:.4f}g CO2")
        return "\n".join(explanation)


class Strategies:
    """Container for query execution strategies"""

    @staticmethod
    def latency_first(urgency: int, carbon: float) -> str:
        """Strategy A: Always FAST"""
        return "fast"

    @staticmethod
    def carbon_deferred(urgency: int, carbon: float) -> str:
        """Strategy B: Carbon‑Aware Deferred"""
        # 1. Critical/High urgency – prioritize performance/completion
        if urgency >= 4:  # High or Critical
            return "balanced"
        # 2. Low urgency + high carbon – defer
        THRESHOLD_CARBON = 400
        THRESHOLD_URGENCY = 3  # Below MEDIUM (i.e., LOW or BATCH)
        if carbon > THRESHOLD_CARBON and urgency < THRESHOLD_URGENCY:
            return "defer"
        # 3. Default
        return "balanced"

    @staticmethod
    def balanced_hybrid(urgency: int, carbon: float) -> str:
        """Strategy C: Balanced Hybrid"""
        return "efficient"


def select_execution_strategy(urgency: int, carbon_intensity: float) -> str:
    """
    Select execution strategy based on urgency and carbon intensity.
    Defaults to Strategy B (Carbon‑Aware Deferred).
    """
    return Strategies.carbon_deferred(urgency, carbon_intensity)


class CarbonAwareSelector:
    """Intelligent selector that chooses query execution variant"""

    def __init__(self, carbon_api: Optional[CarbonAPI] = None):
        self.carbon_api = carbon_api or CarbonAPI()
        self.LOW_CARBON = 250
        self.HIGH_CARBON = 500

    def select(self, context: SelectionContext) -> SelectionDecision:
        """Select optimal variant based on context"""
        urgency_val = context.urgency.to_int()
        carbon_val = context.carbon_intensity.value
        decision_str = select_execution_strategy(urgency_val, carbon_val)
        if decision_str == "fast":
            return self._select_fast(context, "Strategy selected: FAST (Latency‑First)")
        if decision_str == "efficient":
            return self._select_efficient(
                context, "Strategy selected: EFFICIENT (Balanced Hybrid)"
            )
        if decision_str == "defer":
            return self._select_defer(context, carbon_val)
        # Default to balanced
        return self._select_balanced(
            context, f"Strategy selected: {decision_str.upper()}"
        )

    def _select_fast(self, context: SelectionContext, reason: str) -> SelectionDecision:
        """Select FAST variant (equivalent to old _select_critical logic)"""
        strategy = ExecutionStrategy.FAST
        variant = context.available_variants[strategy]
        return SelectionDecision(
            selected_strategy=strategy,
            selected_variant=variant,
            reason=reason,
            expected_energy=variant.estimated_energy,
            expected_carbon=self._estimate_carbon(variant, context.carbon_intensity),
        )

    def _select_efficient(
        self, context: SelectionContext, reason: str
    ) -> SelectionDecision:
        """Select EFFICIENT variant"""
        strategy = ExecutionStrategy.EFFICIENT
        variant = context.available_variants[strategy]
        return SelectionDecision(
            selected_strategy=strategy,
            selected_variant=variant,
            reason=reason,
            expected_energy=variant.estimated_energy,
            expected_carbon=self._estimate_carbon(variant, context.carbon_intensity),
        )

    def _select_balanced(
        self, context: SelectionContext, reason: str
    ) -> SelectionDecision:
        """Select BALANCED variant"""
        strategy = ExecutionStrategy.BALANCED
        variant = context.available_variants[strategy]
        return SelectionDecision(
            selected_strategy=strategy,
            selected_variant=variant,
            reason=reason,
            expected_energy=variant.estimated_energy,
            expected_carbon=self._estimate_carbon(variant, context.carbon_intensity),
        )

    def _select_defer(
        self, context: SelectionContext, current_carbon: float
    ) -> SelectionDecision:
        """Handle deferral logic with a conservative carbon forecast.

        Uses the forecast lower bound (value − uncertainty) to decide whether
        deferral is necessary. If the lower bound is below the threshold (400),
        the query proceeds without deferral. Otherwise we attempt to find a
        better future time within the next 6 hours.
        """
        strategy = ExecutionStrategy.BALANCED  # defer implies BALANCED later
        variant = context.available_variants[strategy]
        defer_minutes = 60  # default
        reason = (
            f"Carbon ({current_carbon:.0f}) > 400. Deferring 60m (forecast unavailable)"
        )
        try:
            forecast = self.carbon_api.get_forecast(hours=6)
            if forecast:
                # Find the forecast entry with the lowest carbon intensity
                min_forecast = min(forecast, key=lambda f: f.value)
                lower_bound = min_forecast.value - min_forecast.uncertainty
                if lower_bound < 400:
                    defer_minutes = 0
                    reason = (
                        f"Carbon ({current_carbon:.0f}) > 400 but forecast lower bound "
                        f"({lower_bound:.0f}) < 400 – proceeding without deferral."
                    )
                else:
                    # Calculate wait time until that forecasted moment
                    time_diff = (
                        min_forecast.timestamp - context.carbon_intensity.timestamp
                    )
                    defer_minutes = max(15, int(time_diff.total_seconds() / 60))
                    reason = (
                        f"Carbon ({current_carbon:.0f}) > 400. Deferring {defer_minutes}m "
                        f"for lower carbon ({min_forecast.value:.0f}±{min_forecast.uncertainty:.0f})"
                    )
        except Exception as e:
            logger.warning(f"Failed to get forecast: {e}")
        return SelectionDecision(
            selected_strategy=strategy,
            selected_variant=variant,
            reason=reason,
            should_defer=defer_minutes > 0,
            defer_minutes=defer_minutes,
            expected_energy=variant.estimated_energy,
            expected_carbon=self._estimate_carbon(variant, context.carbon_intensity),
        )

    def _estimate_carbon(
        self, variant: QueryVariant, carbon_intensity: CarbonIntensity
    ) -> float:
        if variant.estimated_energy is None:
            return 0.0
        kwh = variant.estimated_energy / 3_600_000
        return kwh * carbon_intensity.value
