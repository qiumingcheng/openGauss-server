#pragma once
#include <string>

namespace cascades {


class CostEstimate {
public:
    CostEstimate();
    CostEstimate(double cpu_cost, double memory_cost, double network_cost);
    ~CostEstimate() = default;

    static CostEstimate infinite();

    double cpu_cost() const { return cpu_cost_; }
    void set_cpu_cost(double cost) { cpu_cost_ = cost; }

    double memory_cost() const { return memory_cost_; }
    void set_memory_cost(double cost) { memory_cost_ = cost; }

    double network_cost() const { return network_cost_; }
    void set_network_cost(double cost) { network_cost_ = cost; }

    double total_cost() const;
    bool operator<(const CostEstimate& other) const;
    bool operator>(const CostEstimate& other) const;
    bool operator==(const CostEstimate& other) const;
    CostEstimate& operator+=(const CostEstimate& other);
    CostEstimate operator+(const CostEstimate& other) const;
    CostEstimate operator*(double factor) const;

    std::string to_string() const;

private:
    double cpu_cost_;
    double memory_cost_;
    double network_cost_;
};

}