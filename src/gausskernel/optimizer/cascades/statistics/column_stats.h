#pragma once
#include <memory>

namespace cascades {


class ColumnStats {
public:
    ColumnStats() = default;
    ColumnStats(double null_fraction, double ndv, double avg_width)
        : null_fraction_(null_fraction), ndv_(ndv), avg_width_(avg_width) {}
    ~ColumnStats() = default;

    double null_fraction() const { return null_fraction_; }
    double ndv() const { return ndv_; }
    double avg_width() const { return avg_width_; }

    void set_null_fraction(double f) { null_fraction_ = f; }
    void set_ndv(double n) { ndv_ = n; }
    void set_avg_width(double w) { avg_width_ = w; }

private:
    double null_fraction_ = 0.0;
    double ndv_ = 0.0;
    double avg_width_ = 0.0;
};
}