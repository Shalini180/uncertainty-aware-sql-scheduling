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
        """Human-readable explanation"""
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


class CarbonAwareSelector:
    """Intelligent selector that chooses query execution variant"""

    def __init__(self, carbon_api: Optional[CarbonAPI] = None):
        self.carbon_api = carbon_api or CarbonAPI()
        self.LOW_CARBON = 250
        self.HIGH_CARBON = 500

    def select(self, context: SelectionContext) -> SelectionDecision:
        """Select optimal variant based on context"""
        carbon_value = context.carbon_intensity.value

        # Rule 1: CRITICAL queries always use FAST
        if context.urgency == QueryUrgency.CRITICAL:
            return self._select_critical(context)

        # Rule 2: BATCH queries can be deferred or use EFFICIENT
        if context.urgency == QueryUrgency.BATCH:
            return self._select_batch(context)

        # Rule 3: Adapt to carbon intensity
        if carbon_value < self.LOW_CARBON:
            return self._select_low_carbon(context)
        elif carbon_value > self.HIGH_CARBON:
            return self._select_high_carbon(context)
        else:
            return self._select_medium_carbon(context)

    def _select_critical(self, context: SelectionContext) -> SelectionDecision:
        strategy = ExecutionStrategy.FAST
        variant = context.available_variants[strategy]
        return SelectionDecision(
            selected_strategy=strategy,
            selected_variant=variant,
            reason=f"Critical query requires minimum latency (SLA < 100ms)",
            expected_energy=variant.estimated_energy,
            expected_carbon=self._estimate_carbon(variant, context.carbon_intensity),
        )

    def _select_batch(self, context: SelectionContext) -> SelectionDecision:
        strategy = ExecutionStrategy.EFFICIENT
        variant = context.available_variants[strategy]

        forecast = self.carbon_api.get_forecast(hours=6)
        current_carbon = context.carbon_intensity.value
        min_forecast = min(forecast, key=lambda f: f.value)

        if min_forecast.value < current_carbon * 0.7:
            hours_to_wait = (
                min_forecast.timestamp - context.carbon_intensity.timestamp
            ).total_seconds() / 3600
            return SelectionDecision(
                selected_strategy=strategy,
                selected_variant=variant,
                reason=f"Batch query - deferring for lower carbon ({current_carbon:.0f} → {min_forecast.value:.0f} gCO2/kWh)",
                should_defer=True,
                defer_minutes=int(hours_to_wait * 60),
            )

        return SelectionDecision(
            selected_strategy=strategy,
            selected_variant=variant,
            reason=f"Batch query using energy-efficient variant (carbon: {current_carbon:.0f} gCO2/kWh)",
            expected_energy=variant.estimated_energy,
            expected_carbon=self._estimate_carbon(variant, context.carbon_intensity),
        )

    def _select_low_carbon(self, context: SelectionContext) -> SelectionDecision:
        carbon_value = context.carbon_intensity.value
        if context.urgency == QueryUrgency.HIGH:
            strategy = ExecutionStrategy.FAST
            reason = (
                f"Low carbon ({carbon_value:.0f} gCO2/kWh) + high urgency → using FAST"
            )
        else:
            strategy = ExecutionStrategy.BALANCED
            reason = f"Low carbon ({carbon_value:.0f} gCO2/kWh) → using BALANCED"

        variant = context.available_variants[strategy]
        return SelectionDecision(
            selected_strategy=strategy,
            selected_variant=variant,
            reason=reason,
            expected_energy=variant.estimated_energy,
            expected_carbon=self._estimate_carbon(variant, context.carbon_intensity),
        )

    def _select_high_carbon(self, context: SelectionContext) -> SelectionDecision:
        carbon_value = context.carbon_intensity.value
        if context.urgency == QueryUrgency.HIGH:
            strategy = ExecutionStrategy.BALANCED
            reason = f"High carbon ({carbon_value:.0f} gCO2/kWh) + high urgency → using BALANCED compromise"
        else:
            strategy = ExecutionStrategy.EFFICIENT
            reason = f"High carbon ({carbon_value:.0f} gCO2/kWh) → minimizing energy with EFFICIENT"

        variant = context.available_variants[strategy]
        return SelectionDecision(
            selected_strategy=strategy,
            selected_variant=variant,
            reason=reason,
            expected_energy=variant.estimated_energy,
            expected_carbon=self._estimate_carbon(variant, context.carbon_intensity),
        )

    def _select_medium_carbon(self, context: SelectionContext) -> SelectionDecision:
        carbon_value = context.carbon_intensity.value
        strategy = ExecutionStrategy.BALANCED
        variant = context.available_variants[strategy]
        return SelectionDecision(
            selected_strategy=strategy,
            selected_variant=variant,
            reason=f"Medium carbon ({carbon_value:.0f} gCO2/kWh) → using BALANCED",
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
