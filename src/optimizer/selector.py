# src/optimizer/selector.py
from enum import Enum
from typing import Dict, Optional
from dataclasses import dataclass
import logging

from src.core.compiler import ExecutionStrategy, QueryVariant
from src.optimizer.carbon_api import CarbonAPI, CarbonIntensity

logger = logging.getLogger(__name__)


class QueryUrgency(Enum):
    """Query urgency levels for SLA-based decisions"""

    CRITICAL = "critical"  # <100ms SLO, always use FAST
    HIGH = "high"  # <500ms SLO
    MEDIUM = "medium"  # <2s SLO (default)
    LOW = "low"  # <10s SLO
    BATCH = "batch"  # Can defer indefinitely


@dataclass
class SelectionContext:
    """Context for making variant selection decision"""

    query: str
    urgency: QueryUrgency
    carbon_intensity: CarbonIntensity
    available_variants: Dict[ExecutionStrategy, QueryVariant]
    current_load: Optional[float] = None  # System load 0-100%


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
    """
    Intelligent selector that chooses query execution variant
    based on carbon intensity, urgency, and system state
    """

    def __init__(self, carbon_api: Optional[CarbonAPI] = None):
        self.carbon_api = carbon_api or CarbonAPI()

        # Thresholds (configurable)
        self.LOW_CARBON = 250  # gCO2/kWh
        self.HIGH_CARBON = 500  # gCO2/kWh

        logger.info("CarbonAwareSelector initialized")

    def select(self, context: SelectionContext) -> SelectionDecision:
        """
        Select optimal variant based on context

        Args:
            context: Selection context with query, urgency, carbon, etc.

        Returns:
            Decision with selected strategy and reasoning
        """
        carbon_value = context.carbon_intensity.value

        # Rule 1: CRITICAL queries always use FAST (SLA > environment)
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
        """Critical queries: Always use FAST"""
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
        """Batch queries: Use EFFICIENT or defer"""
        strategy = ExecutionStrategy.EFFICIENT
        variant = context.available_variants[strategy]

        # Check if we should defer
        forecast = self.carbon_api.get_forecast(hours=6)
        current_carbon = context.carbon_intensity.value
        min_forecast = min(forecast, key=lambda f: f.value)

        # If carbon will drop significantly, recommend deferral
        if min_forecast.value < current_carbon * 0.7:  # 30% improvement
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
        """Low carbon: Can afford to use FAST"""
        carbon_value = context.carbon_intensity.value

        # Use FAST for HIGH urgency, BALANCED for others
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
        """High carbon: Prioritize energy efficiency"""
        carbon_value = context.carbon_intensity.value

        # Even HIGH urgency should consider efficiency
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
        """Medium carbon: Use BALANCED"""
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
        """Estimate carbon emissions in grams CO2"""
        if variant.estimated_energy is None:
            return 0.0

        # Convert joules to kWh and multiply by carbon intensity
        kwh = variant.estimated_energy / 3_600_000
        return kwh * carbon_intensity.value


# Test it
if __name__ == "__main__":
    from src.core.compiler import MultiVariantCompiler

    logging.basicConfig(level=logging.INFO)

    # Setup
    carbon_api = CarbonAPI()
    selector = CarbonAwareSelector(carbon_api)
    compiler = MultiVariantCompiler()

    # Test query
    sql = "SELECT * FROM large_table WHERE id > 1000"
    variants = compiler.compile(sql)
    carbon = carbon_api.get_current_intensity()

    # Test different urgencies
    urgencies = [
        QueryUrgency.CRITICAL,
        QueryUrgency.HIGH,
        QueryUrgency.MEDIUM,
        QueryUrgency.LOW,
        QueryUrgency.BATCH,
    ]

    print(f"Current carbon intensity: {carbon.value:.0f} gCO2/kWh\n")

    for urgency in urgencies:
        context = SelectionContext(
            query=sql,
            urgency=urgency,
            carbon_intensity=carbon,
            available_variants=variants,
        )

        decision = selector.select(context)
        print(f"{urgency.value.upper()}:")
        print(decision.explain())
        print()
