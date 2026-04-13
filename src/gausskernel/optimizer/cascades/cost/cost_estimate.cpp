#include "cost_estimate.h"

namespace cascades {
CostEstimate::CostEstimate() : cpu_cost_(0.0), memory_cost_(0.0), network_cost_(0.0) {}

CostEstimate::CostEstimate(double cpu_cost, double memory_cost, double network_cost)
    : cpu_cost_(cpu_cost), memory_cost_(memory_cost), network_cost_(network_cost) {}

CostEstimate CostEstimate::infinite() {
    return CostEstimate(1e30, 1e30, 1e30);
}

double CostEstimate::total_cost() const {
    return cpu_cost_ + memory_cost_ + network_cost_;
}

bool CostEstimate::operator<(const CostEstimate& other) const {
    return total_cost() < other.total_cost();
}

bool CostEstimate::operator>(const CostEstimate& other) const {
    return total_cost() > other.total_cost();
}

bool CostEstimate::operator==(const CostEstimate& other) const {
    return cpu_cost_ == other.cpu_cost_ &&
           memory_cost_ == other.memory_cost_ &&
           network_cost_ == other.network_cost_;
}

CostEstimate& CostEstimate::operator+=(const CostEstimate& other) {
    cpu_cost_ += other.cpu_cost_;
    memory_cost_ += other.memory_cost_;
    network_cost_ += other.network_cost_;
    return *this;
}

CostEstimate CostEstimate::operator+(const CostEstimate& other) const {
    return CostEstimate(cpu_cost_ + other.cpu_cost_,
                       memory_cost_ + other.memory_cost_,
                       network_cost_ + other.network_cost_);
}

CostEstimate CostEstimate::operator*(double factor) const {
    return CostEstimate(cpu_cost_ * factor,
                       memory_cost_ * factor,
                       network_cost_ * factor);
}

std::string CostEstimate::to_string() const {
    return "CostEstimate(cpu=" + std::to_string(cpu_cost_) +
           ", mem=" + std::to_string(memory_cost_) +
           ", net=" + std::to_string(network_cost_) + ")";
}


}  // namespace cascades