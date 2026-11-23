/* Private adaptation of Microsoft DiskANN cpp_main
 * 78256bbab4685e1774e78d331e081a153be26823.
 * Numerical loops unchanged. Changes: equivalent array ownership, checked
 * output transfer, return-code cancellation polling and private declarations.
 * Source hashes and exact diff are recorded by pq_cpu_component.py.
 *
 *     DiskANN
 *     
 *     MIT License
 * 
 *     Copyright (c) Microsoft Corporation.
 * 
 *     Permission is hereby granted, free of charge, to any person obtaining a copy
 *     of this software and associated documentation files (the "Software"), to deal
 *     in the Software without restriction, including without limitation the rights
 *     to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *     copies of the Software, and to permit persons to whom the Software is
 *     furnished to do so, subject to the following conditions:
 * 
 *     The above copyright notice and this permission notice shall be included in all
 *     copies or substantial portions of the Software.
 * 
 *     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *     IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *     FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *     AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *     LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *     OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *     SOFTWARE
 */
#include "pq/pq_cpu_internal.h"
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <queue>
#include <random>
#include <immintrin.h>
#include <cblas.h>
using MKL_INT=blasint;
namespace pq_cpu_ms {
struct CenterRestore {uint32_t*& pointer;uint32_t* previous;bool committed;~CenterRestore(){if(!committed)pointer=previous;}};
namespace diskann {struct NullLog {template<class T> NullLog& operator<<(const T&){return *this;} NullLog& operator<<(std::ostream&(*)(std::ostream&)){return *this;}};static NullLog cout;}
struct PivotContainer
{
    PivotContainer() = default;

    PivotContainer(size_t pivo_id, float pivo_dist) : piv_id{pivo_id}, piv_dist{pivo_dist}
    {
    }

    bool operator<(const PivotContainer &p) const
    {
        return p.piv_dist < piv_dist;
    }

    bool operator>(const PivotContainer &p) const
    {
        return p.piv_dist > piv_dist;
    }

    size_t piv_id;
    float piv_dist;
};

namespace math_utils
{

float calc_distance(float *vec_1, float *vec_2, size_t dim);

// compute l2-squared norms of data stored in row major num_points * dim,
// needs
// to be pre-allocated
void compute_vecs_l2sq(float *vecs_l2sq, float *data, const size_t num_points, const size_t dim);

void rotate_data_randomly(float *data, size_t num_points, size_t dim, float *rot_mat, float *&new_mat,
                          bool transpose_rot = false);

// calculate closest center to data of num_points * dim (row major)
// centers is num_centers * dim (row major)
// data_l2sq has pre-computed squared norms of data
// centers_l2sq has pre-computed squared norms of centers
// pre-allocated center_index will contain id of k nearest centers
// pre-allocated dist_matrix shound be num_points * num_centers and contain
// squared distances

// Ideally used only by compute_closest_centers
void compute_closest_centers_in_block(const float *const data, const size_t num_points, const size_t dim,
                                      const float *const centers, const size_t num_centers,
                                      const float *const docs_l2sq, const float *const centers_l2sq,
                                      uint32_t *center_index, float *const dist_matrix, size_t k = 1);

// Given data in num_points * new_dim row major
// Pivots stored in full_pivot_data as k * new_dim row major
// Calculate the closest pivot for each point and store it in vector
// closest_centers_ivf (which needs to be allocated outside)
// Additionally, if inverted index is not null (and pre-allocated), it will
// return inverted index for each center Additionally, if pts_norms_squared is
// not null, then it will assume that point norms are pre-computed and use
// those
// values

void compute_closest_centers(float *data, size_t num_points, size_t dim, float *pivot_data, size_t num_centers,
                             size_t k, uint32_t *closest_centers_ivf, std::vector<size_t> *inverted_index = NULL,
                             float *pts_norms_squared = NULL);

// if to_subtract is 1, will subtract nearest center from each row. Else will
// add. Output will be in data_load iself.
// Nearest centers need to be provided in closst_centers.

void process_residuals(float *data_load, size_t num_points, size_t dim, float *cur_pivot_data, size_t num_centers,
                       uint32_t *closest_centers, bool to_subtract);

} // namespace math_utils

namespace kmeans
{

// run Lloyds one iteration
// Given data in row major num_points * dim, and centers in row major
// num_centers * dim
// And squared lengths of data points, output the closest center to each data
// point, update centers, and also return inverted index.
// If closest_centers == NULL, will allocate memory and return.
// Similarly, if closest_docs == NULL, will allocate memory and return.

float lloyds_iter(float *data, size_t num_points, size_t dim, float *centers, size_t num_centers, float *docs_l2sq,
                  std::vector<size_t> *closest_docs, uint32_t *&closest_center);

// Run Lloyds until max_reps or stopping criterion
// If you pass NULL for closest_docs and closest_center, it will NOT return
// the results, else it will assume appriate allocation as closest_docs = new
// vector<size_t> [num_centers], and closest_center = new size_t[num_points]
// Final centers are output in centers as row major num_centers * dim
//
float run_lloyds(float *data, size_t num_points, size_t dim, float *centers, const size_t num_centers,
                 const size_t max_reps, std::vector<size_t> *closest_docs, uint32_t *closest_center);

// assumes already memory allocated for pivot_data as new
// float[num_centers*dim] and select randomly num_centers points as pivots
void selecting_pivots(float *data, size_t num_points, size_t dim, float *pivot_data, size_t num_centers);

void kmeanspp_selecting_pivots(float *data, size_t num_points, size_t dim, float *pivot_data, size_t num_centers);
} // namespace kmeans

namespace math_utils
{

float calc_distance(float *vec_1, float *vec_2, size_t dim)
{
    float dist = 0;
    for (size_t j = 0; j < dim; j++)
    {
        dist += (vec_1[j] - vec_2[j]) * (vec_1[j] - vec_2[j]);
    }
    return dist;
}

// compute l2-squared norms of data stored in row major num_points * dim,
// needs
// to be pre-allocated
void compute_vecs_l2sq(float *vecs_l2sq, float *data, const size_t num_points, const size_t dim)
{
#pragma omp parallel for schedule(static, 8192)
    for (int64_t n_iter = 0; n_iter < (int64_t)num_points; n_iter++)
    {
        vecs_l2sq[n_iter] = cblas_snrm2((MKL_INT)dim, (data + (n_iter * dim)), 1);
        vecs_l2sq[n_iter] *= vecs_l2sq[n_iter];
    }
}

void rotate_data_randomly(float *data, size_t num_points, size_t dim, float *rot_mat, float *&new_mat,
                          bool transpose_rot)
{
    CBLAS_TRANSPOSE transpose = CblasNoTrans;
    if (transpose_rot)
    {
        diskann::cout << "Transposing rotation matrix.." << std::flush;
        transpose = CblasTrans;
    }
    diskann::cout << "done Rotating data with random matrix.." << std::flush;

    Poll();

    cblas_sgemm(CblasRowMajor, CblasNoTrans, transpose, (MKL_INT)num_points, (MKL_INT)dim, (MKL_INT)dim, 1.0, data,
                (MKL_INT)dim, rot_mat, (MKL_INT)dim, 0, new_mat, (MKL_INT)dim);

    diskann::cout << "done." << std::endl;
}

// calculate k closest centers to data of num_points * dim (row major)
// centers is num_centers * dim (row major)
// data_l2sq has pre-computed squared norms of data
// centers_l2sq has pre-computed squared norms of centers
// pre-allocated center_index will contain id of nearest center
// pre-allocated dist_matrix shound be num_points * num_centers and contain
// squared distances
// Default value of k is 1

// Ideally used only by compute_closest_centers
void compute_closest_centers_in_block(const float *const data, const size_t num_points, const size_t dim,
                                      const float *const centers, const size_t num_centers,
                                      const float *const docs_l2sq, const float *const centers_l2sq,
                                      uint32_t *center_index, float *const dist_matrix, size_t k)
{
    if (k > num_centers)
    {
        diskann::cout << "ERROR: k (" << k << ") > num_center(" << num_centers << ")" << std::endl;
        return;
    }

    std::unique_ptr<float[]> ones_a_owner(new float[num_centers]);
    float *ones_a=ones_a_owner.get();
    std::unique_ptr<float[]> ones_b_owner(new float[num_points]);
    float *ones_b=ones_b_owner.get();

    for (size_t i = 0; i < num_centers; i++)
    {
        ones_a[i] = 1.0;
    }
    for (size_t i = 0; i < num_points; i++)
    {
        ones_b[i] = 1.0;
    }

    Poll();

    cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasTrans, (MKL_INT)num_points, (MKL_INT)num_centers, (MKL_INT)1, 1.0f,
                docs_l2sq, (MKL_INT)1, ones_a, (MKL_INT)1, 0.0f, dist_matrix, (MKL_INT)num_centers);

    Poll();

    cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasTrans, (MKL_INT)num_points, (MKL_INT)num_centers, (MKL_INT)1, 1.0f,
                ones_b, (MKL_INT)1, centers_l2sq, (MKL_INT)1, 1.0f, dist_matrix, (MKL_INT)num_centers);

    Poll();

    cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasTrans, (MKL_INT)num_points, (MKL_INT)num_centers, (MKL_INT)dim, -2.0f,
                data, (MKL_INT)dim, centers, (MKL_INT)dim, 1.0f, dist_matrix, (MKL_INT)num_centers);

    if (k == 1)
    {
#pragma omp parallel for schedule(static, 8192)
        for (int64_t i = 0; i < (int64_t)num_points; i++)
        {
            float min = std::numeric_limits<float>::max();
            float *current = dist_matrix + (i * num_centers);
            for (size_t j = 0; j < num_centers; j++)
            {
                if (current[j] < min)
                {
                    center_index[i] = (uint32_t)j;
                    min = current[j];
                }
            }
        }
    }
    else
    {
#pragma omp parallel for schedule(static, 8192)
        for (int64_t i = 0; i < (int64_t)num_points; i++)
        {
            std::priority_queue<PivotContainer> top_k_queue;
            float *current = dist_matrix + (i * num_centers);
            for (size_t j = 0; j < num_centers; j++)
            {
                PivotContainer this_piv(j, current[j]);
                top_k_queue.push(this_piv);
            }
            for (size_t j = 0; j < k; j++)
            {
                PivotContainer this_piv = top_k_queue.top();
                center_index[i * k + j] = (uint32_t)this_piv.piv_id;
                top_k_queue.pop();
            }
        }
    }
    ones_a_owner.reset();
    ones_b_owner.reset();
}

// Given data in num_points * new_dim row major
// Pivots stored in full_pivot_data as num_centers * new_dim row major
// Calculate the k closest pivot for each point and store it in vector
// closest_centers_ivf (row major, num_points*k) (which needs to be allocated
// outside) Additionally, if inverted index is not null (and pre-allocated),
// it
// will return inverted index for each center, assuming each of the inverted
// indices is an empty vector. Additionally, if pts_norms_squared is not null,
// then it will assume that point norms are pre-computed and use those values

void compute_closest_centers(float *data, size_t num_points, size_t dim, float *pivot_data, size_t num_centers,
                             size_t k, uint32_t *closest_centers_ivf, std::vector<size_t> *inverted_index,
                             float *pts_norms_squared)
{
    if (k > num_centers)
    {
        diskann::cout << "ERROR: k (" << k << ") > num_center(" << num_centers << ")" << std::endl;
        return;
    }

    bool is_norm_given_for_pts = (pts_norms_squared != NULL);

    std::unique_ptr<float[]> pivs_norms_squared_owner(new float[num_centers]);
    float *pivs_norms_squared=pivs_norms_squared_owner.get();
    std::unique_ptr<float[]> pts_norms_squared_owner;
    if (!is_norm_given_for_pts) { pts_norms_squared_owner.reset(new float[num_points]); pts_norms_squared=pts_norms_squared_owner.get(); }

    size_t PAR_BLOCK_SIZE = num_points;
    size_t N_BLOCKS =
        (num_points % PAR_BLOCK_SIZE) == 0 ? (num_points / PAR_BLOCK_SIZE) : (num_points / PAR_BLOCK_SIZE) + 1;

    if (!is_norm_given_for_pts)
        math_utils::compute_vecs_l2sq(pts_norms_squared, data, num_points, dim);
    math_utils::compute_vecs_l2sq(pivs_norms_squared, pivot_data, num_centers, dim);
    std::unique_ptr<uint32_t[]> closest_centers_owner(new uint32_t[PAR_BLOCK_SIZE * k]);
    uint32_t *closest_centers=closest_centers_owner.get();
    std::unique_ptr<float[]> distance_matrix_owner(new float[num_centers * PAR_BLOCK_SIZE]);
    float *distance_matrix=distance_matrix_owner.get();

    for (size_t cur_blk = 0; cur_blk < N_BLOCKS; cur_blk++)
    {
        Poll();
        float *data_cur_blk = data + cur_blk * PAR_BLOCK_SIZE * dim;
        size_t num_pts_blk = std::min(PAR_BLOCK_SIZE, num_points - cur_blk * PAR_BLOCK_SIZE);
        float *pts_norms_blk = pts_norms_squared + cur_blk * PAR_BLOCK_SIZE;

        math_utils::compute_closest_centers_in_block(data_cur_blk, num_pts_blk, dim, pivot_data, num_centers,
                                                     pts_norms_blk, pivs_norms_squared, closest_centers,
                                                     distance_matrix, k);

#pragma omp parallel for schedule(static, 1)
        for (int64_t j = cur_blk * PAR_BLOCK_SIZE;
             j < std::min((int64_t)num_points, (int64_t)((cur_blk + 1) * PAR_BLOCK_SIZE)); j++)
        {
            for (size_t l = 0; l < k; l++)
            {
                size_t this_center_id = closest_centers[(j - cur_blk * PAR_BLOCK_SIZE) * k + l];
                closest_centers_ivf[j * k + l] = (uint32_t)this_center_id;
                if (inverted_index != NULL)
                {
#pragma omp critical
                    inverted_index[this_center_id].push_back(j);
                }
            }
        }
    }
    closest_centers_owner.reset();
    distance_matrix_owner.reset();
    pivs_norms_squared_owner.reset();
    if (!is_norm_given_for_pts)
        pts_norms_squared_owner.reset();
}

// if to_subtract is 1, will subtract nearest center from each row. Else will
// add. Output will be in data_load iself.
// Nearest centers need to be provided in closst_centers.
void process_residuals(float *data_load, size_t num_points, size_t dim, float *cur_pivot_data, size_t num_centers,
                       uint32_t *closest_centers, bool to_subtract)
{
    diskann::cout << "Processing residuals of " << num_points << " points in " << dim << " dimensions using "
                  << num_centers << " centers " << std::endl;
#pragma omp parallel for schedule(static, 8192)
    for (int64_t n_iter = 0; n_iter < (int64_t)num_points; n_iter++)
    {
        for (size_t d_iter = 0; d_iter < dim; d_iter++)
        {
            if (to_subtract == 1)
                data_load[n_iter * dim + d_iter] =
                    data_load[n_iter * dim + d_iter] - cur_pivot_data[closest_centers[n_iter] * dim + d_iter];
            else
                data_load[n_iter * dim + d_iter] =
                    data_load[n_iter * dim + d_iter] + cur_pivot_data[closest_centers[n_iter] * dim + d_iter];
        }
    }
}

} // namespace math_utils

namespace kmeans
{

// run Lloyds one iteration
// Given data in row major num_points * dim, and centers in row major
// num_centers * dim And squared lengths of data points, output the closest
// center to each data point, update centers, and also return inverted index.
// If
// closest_centers == NULL, will allocate memory and return. Similarly, if
// closest_docs == NULL, will allocate memory and return.

float lloyds_iter(float *data, size_t num_points, size_t dim, float *centers, size_t num_centers, float *docs_l2sq,
                  std::vector<size_t> *closest_docs, uint32_t *&closest_center)
{
    bool compute_residual = true;
    // Timer timer;

    std::unique_ptr<uint32_t[]> optional_center_owner;
    CenterRestore center_restore{closest_center,closest_center,false};
    std::unique_ptr<std::vector<size_t>[]> optional_docs_owner;
    if (closest_center == NULL) { optional_center_owner.reset(new uint32_t[num_points]); closest_center=optional_center_owner.get(); }
    if (closest_docs == NULL) { optional_docs_owner.reset(new std::vector<size_t>[num_centers]); closest_docs=optional_docs_owner.get(); }
    else
        for (size_t c = 0; c < num_centers; ++c)
            closest_docs[c].clear();

    math_utils::compute_closest_centers(data, num_points, dim, centers, num_centers, 1, closest_center, closest_docs,
                                        docs_l2sq);

    memset(centers, 0, sizeof(float) * (size_t)num_centers * (size_t)dim);

#pragma omp parallel for schedule(static, 1)
    for (int64_t c = 0; c < (int64_t)num_centers; ++c)
    {
        Poll();
        float *center = centers + (size_t)c * (size_t)dim;
        std::unique_ptr<double[]> cluster_sum_owner(new double[dim]);
        double *cluster_sum=cluster_sum_owner.get();
        for (size_t i = 0; i < dim; i++)
            cluster_sum[i] = 0.0;
        for (size_t i = 0; i < closest_docs[c].size(); i++)
        {
            float *current = data + ((closest_docs[c][i]) * dim);
            for (size_t j = 0; j < dim; j++)
            {
                cluster_sum[j] += (double)current[j];
            }
        }
        if (closest_docs[c].size() > 0)
        {
            for (size_t i = 0; i < dim; i++)
                center[i] = (float)(cluster_sum[i] / ((double)closest_docs[c].size()));
        }
        cluster_sum_owner.reset();
    }

    float residual = 0.0;
    if (compute_residual)
    {
        size_t BUF_PAD = 32;
        size_t CHUNK_SIZE = 2 * 8192;
        size_t nchunks = num_points / CHUNK_SIZE + (num_points % CHUNK_SIZE == 0 ? 0 : 1);
        std::vector<float> residuals(nchunks * BUF_PAD, 0.0);

#pragma omp parallel for schedule(static, 32)
        for (int64_t chunk = 0; chunk < (int64_t)nchunks; ++chunk)
            for (size_t d = chunk * CHUNK_SIZE; d < num_points && d < (chunk + 1) * CHUNK_SIZE; ++d)
                residuals[chunk * BUF_PAD] +=
                    math_utils::calc_distance(data + (d * dim), centers + (size_t)closest_center[d] * (size_t)dim, dim);

        for (size_t chunk = 0; chunk < nchunks; ++chunk)
            residual += residuals[chunk * BUF_PAD];
    }

    if(optional_center_owner)optional_center_owner.release();
    center_restore.committed=true;
    return residual;
}

// Run Lloyds until max_reps or stopping criterion
// If you pass NULL for closest_docs and closest_center, it will NOT return
// the
// results, else it will assume appriate allocation as closest_docs = new
// vector<size_t> [num_centers], and closest_center = new size_t[num_points]
// Final centers are output in centers as row major num_centers * dim
//
float run_lloyds(float *data, size_t num_points, size_t dim, float *centers, const size_t num_centers,
                 const size_t max_reps, std::vector<size_t> *closest_docs, uint32_t *closest_center)
{
    float residual = std::numeric_limits<float>::max();
    bool ret_closest_docs = true;
    bool ret_closest_center = true;
    std::unique_ptr<std::vector<size_t>[]> closest_docs_owner;
    std::unique_ptr<uint32_t[]> closest_center_owner;
    if (closest_docs == NULL)
    {
        closest_docs_owner.reset(new std::vector<size_t>[num_centers]);
        closest_docs=closest_docs_owner.get();
        ret_closest_docs = false;
    }
    if (closest_center == NULL)
    {
        closest_center_owner.reset(new uint32_t[num_points]);
        closest_center=closest_center_owner.get();
        ret_closest_center = false;
    }

    std::unique_ptr<float[]> docs_l2sq_owner(new float[num_points]);
    float *docs_l2sq=docs_l2sq_owner.get();
    math_utils::compute_vecs_l2sq(docs_l2sq, data, num_points, dim);

    float old_residual;
    // Timer timer;
    for (size_t i = 0; i < max_reps; ++i)
    {
        Poll();
        old_residual = residual;

        residual = lloyds_iter(data, num_points, dim, centers, num_centers, docs_l2sq, closest_docs, closest_center);

        if (((i != 0) && ((old_residual - residual) / residual) < 0.00001) ||
            (residual < std::numeric_limits<float>::epsilon()))
        {
            diskann::cout << "Residuals unchanged: " << old_residual << " becomes " << residual
                          << ". Early termination." << std::endl;
            break;
        }
    }
    docs_l2sq_owner.reset();
    if (!ret_closest_docs)
        closest_docs_owner.reset();
    if (!ret_closest_center)
        closest_center_owner.reset();
    return residual;
}

// assumes memory allocated for pivot_data as new
// float[num_centers*dim]
// and select randomly num_centers points as pivots
void selecting_pivots(float *data, size_t num_points, size_t dim, float *pivot_data, size_t num_centers)
{
    //	pivot_data = new float[num_centers * dim];

    std::vector<size_t> picked;
    std::random_device rd;
    auto x = rd();
    std::mt19937 generator(x);
    std::uniform_int_distribution<size_t> distribution(0, num_points - 1);

    size_t tmp_pivot;
    for (size_t j = 0; j < num_centers; j++)
    {
        tmp_pivot = distribution(generator);
        if (std::find(picked.begin(), picked.end(), tmp_pivot) != picked.end())
            continue;
        picked.push_back(tmp_pivot);
        std::memcpy(pivot_data + j * dim, data + tmp_pivot * dim, dim * sizeof(float));
    }
}

void kmeanspp_selecting_pivots(float *data, size_t num_points, size_t dim, float *pivot_data, size_t num_centers)
{
    if (num_points > 1 << 23)
    {
        diskann::cout << "ERROR: n_pts " << num_points
                      << " currently not supported for k-means++, maximum is "
                         "8388608. Falling back to random pivot "
                         "selection."
                      << std::endl;
        selecting_pivots(data, num_points, dim, pivot_data, num_centers);
        return;
    }

    std::vector<size_t> picked;
    std::random_device rd;
    auto x = rd();
    std::mt19937 generator(x);
    std::uniform_real_distribution<> distribution(0, 1);
    std::uniform_int_distribution<size_t> int_dist(0, num_points - 1);
    size_t init_id = int_dist(generator);
    size_t num_picked = 1;

    picked.push_back(init_id);
    std::memcpy(pivot_data, data + init_id * dim, dim * sizeof(float));

    std::unique_ptr<float[]> dist_owner(new float[num_points]);
    float *dist=dist_owner.get();

#pragma omp parallel for schedule(static, 8192)
    for (int64_t i = 0; i < (int64_t)num_points; i++)
    {
        dist[i] = math_utils::calc_distance(data + i * dim, data + init_id * dim, dim);
    }

    double dart_val;
    size_t tmp_pivot;
    bool sum_flag = false;

    while (num_picked < num_centers)
    {
        Poll();
        dart_val = distribution(generator);

        double sum = 0;
        for (size_t i = 0; i < num_points; i++)
        {
            sum = sum + dist[i];
        }
        if (sum == 0)
            sum_flag = true;

        dart_val *= sum;

        double prefix_sum = 0;
        for (size_t i = 0; i < (num_points); i++)
        {
            tmp_pivot = i;
            if (dart_val >= prefix_sum && dart_val < prefix_sum + dist[i])
            {
                break;
            }

            prefix_sum += dist[i];
        }

        if (std::find(picked.begin(), picked.end(), tmp_pivot) != picked.end() && (sum_flag == false))
            continue;
        picked.push_back(tmp_pivot);
        std::memcpy(pivot_data + num_picked * dim, data + tmp_pivot * dim, dim * sizeof(float));

#pragma omp parallel for schedule(static, 8192)
        for (int64_t i = 0; i < (int64_t)num_points; i++)
        {
            dist[i] = (std::min)(dist[i], math_utils::calc_distance(data + i * dim, data + tmp_pivot * dim, dim));
        }
        num_picked++;
    }
    dist_owner.reset();
}

} // namespace kmeans


namespace diskann {
void FixedChunkPQTable::populate_chunk_distances(const float *query_vec, float *dist_vec)
{
    memset(dist_vec, 0, 256 * n_chunks * sizeof(float));
    // chunk wise distance computation
    for (size_t chunk = 0; chunk < n_chunks; chunk++)
    {
        Poll();
        // sum (q-c)^2 for the dimensions associated with this chunk
        float *chunk_dists = dist_vec + (256 * chunk);
        for (size_t j = chunk_offsets[chunk]; j < chunk_offsets[chunk + 1]; j++)
        {
            const float *centers_dim_vec = tables_tr + (256 * j);
            for (size_t idx = 0; idx < 256; idx++)
            {
                double diff = centers_dim_vec[idx] - (query_vec[j]);
                chunk_dists[idx] += (float)(diff * diff);
            }
        }
    }
}

void FixedChunkPQTable::populate_chunk_inner_products(const float *query_vec, float *dist_vec)
{
    memset(dist_vec, 0, 256 * n_chunks * sizeof(float));
    // chunk wise distance computation
    for (size_t chunk = 0; chunk < n_chunks; chunk++)
    {
        Poll();
        // sum (q-c)^2 for the dimensions associated with this chunk
        float *chunk_dists = dist_vec + (256 * chunk);
        for (size_t j = chunk_offsets[chunk]; j < chunk_offsets[chunk + 1]; j++)
        {
            const float *centers_dim_vec = tables_tr + (256 * j);
            for (size_t idx = 0; idx < 256; idx++)
            {
                double prod = centers_dim_vec[idx] * query_vec[j]; // assumes that we are not
                                                                   // shifting the vectors to
                                                                   // mean zero, i.e., centroid
                                                                   // array should be all zeros
                chunk_dists[idx] -= (float)prod;                   // returning negative to keep the search code
                                                                   // clean (max inner product vs min distance)
            }
        }
    }
}

float FixedChunkPQTable::l2_distance(const float *query_vec, uint8_t *base_vec)
{
    float res = 0;
    for (size_t chunk = 0; chunk < n_chunks; chunk++)
    {
        Poll();
        for (size_t j = chunk_offsets[chunk]; j < chunk_offsets[chunk + 1]; j++)
        {
            const float *centers_dim_vec = tables_tr + (256 * j);
            float diff = centers_dim_vec[base_vec[chunk]] - (query_vec[j]);
            res += diff * diff;
        }
    }
    return res;
}

float FixedChunkPQTable::inner_product(const float *query_vec, uint8_t *base_vec)
{
    float res = 0;
    for (size_t chunk = 0; chunk < n_chunks; chunk++)
    {
        Poll();
        for (size_t j = chunk_offsets[chunk]; j < chunk_offsets[chunk + 1]; j++)
        {
            const float *centers_dim_vec = tables_tr + (256 * j);
            float diff = centers_dim_vec[base_vec[chunk]] * query_vec[j]; // assumes centroid is 0 to
                                                                          // prevent translation errors
            res += diff;
        }
    }
    return -res; // returns negative value to simulate distances (max -> min
                 // conversion)
}

void FixedChunkPQTable::inflate_vector(uint8_t *base_vec, float *out_vec)
{
    for (size_t chunk = 0; chunk < n_chunks; chunk++)
    {
        Poll();
        for (size_t j = chunk_offsets[chunk]; j < chunk_offsets[chunk + 1]; j++)
        {
            const float *centers_dim_vec = tables_tr + (256 * j);
            out_vec[j] = centers_dim_vec[base_vec[chunk]] + centroid[j];
        }
    }
}

int generate_pq_pivots_simplified(const float *train_data, size_t num_train, size_t dim, size_t num_pq_chunks,
                                  std::vector<float> &pivot_data_vector)
{
    if (num_pq_chunks > dim || dim % num_pq_chunks != 0)
    {
        return -1;
    }

    const size_t num_centers = 256;
    const size_t cur_chunk_size = dim / num_pq_chunks;
    const uint32_t KMEANS_ITERS_FOR_PQ = 15;

    pivot_data_vector.resize(num_centers * dim);
    std::vector<float> cur_pivot_data_vector(num_centers * cur_chunk_size);
    std::vector<float> cur_data_vector(num_train * cur_chunk_size);
    std::vector<uint32_t> closest_center_vector(num_train);

    float *pivot_data = &pivot_data_vector[0];
    float *cur_pivot_data = &cur_pivot_data_vector[0];
    float *cur_data = &cur_data_vector[0];
    uint32_t *closest_center = &closest_center_vector[0];

    for (size_t i = 0; i < num_pq_chunks; i++)
    {
        Poll();
        size_t chunk_offset = cur_chunk_size * i;

        for (int32_t j = 0; j < num_train; j++)
        {
            std::memcpy(cur_data + j * cur_chunk_size, train_data + j * dim + chunk_offset,
                        cur_chunk_size * sizeof(float));
        }

        kmeans::kmeanspp_selecting_pivots(cur_data, num_train, cur_chunk_size, cur_pivot_data, num_centers);

        kmeans::run_lloyds(cur_data, num_train, cur_chunk_size, cur_pivot_data, num_centers, KMEANS_ITERS_FOR_PQ, NULL,
                           closest_center);

        for (uint64_t j = 0; j < num_centers; j++)
        {
            std::memcpy(pivot_data + j * dim + chunk_offset, cur_pivot_data + j * cur_chunk_size,
                        cur_chunk_size * sizeof(float));
        }
    }

    return 0;
}

int generate_pq_data_from_pivots_simplified(const float *data, const size_t num, const float *pivot_data,
                                            const size_t pivots_num, const size_t dim, const size_t num_pq_chunks,
                                            std::vector<uint8_t> &pq)
{
    if (num_pq_chunks == 0 || num_pq_chunks > dim || dim % num_pq_chunks != 0)
    {
        return -1;
    }

    const size_t num_centers = 256;
    const size_t chunk_size = dim / num_pq_chunks;

    if (pivots_num != num_centers * dim)
    {
        return -1;
    }

    pq.resize(num * num_pq_chunks);

    std::vector<float> cur_pivot_vector(num_centers * chunk_size);
    std::vector<float> cur_data_vector(num * chunk_size);
    std::vector<uint32_t> closest_center_vector(num);

    float *cur_pivot_data = &cur_pivot_vector[0];
    float *cur_data = &cur_data_vector[0];
    uint32_t *closest_center = &closest_center_vector[0];

    for (size_t i = 0; i < num_pq_chunks; i++)
    {
        Poll();
        const size_t chunk_offset = chunk_size * i;

        for (int j = 0; j < num_centers; j++)
        {
            std::memcpy(cur_pivot_data + j * chunk_size, pivot_data + j * dim + chunk_offset,
                        chunk_size * sizeof(float));
        }

        for (int j = 0; j < num; j++)
        {
            for (size_t k = 0; k < chunk_size; k++)
            {
                cur_data[j * chunk_size + k] = data[j * dim + chunk_offset + k];
            }
        }

        math_utils::compute_closest_centers(cur_data, num, chunk_size, cur_pivot_data, num_centers, 1, closest_center);

        for (int j = 0; j < num; j++)
        {
            assert(closest_center[j] < num_centers);
            pq[j * num_pq_chunks + i] = closest_center[j];
        }
    }

    return 0;
}

void pq_dist_lookup(const uint8_t *pq_ids, const size_t n_pts, const size_t pq_nchunks, const float *pq_dists,
                    float *dists_out)
{
    _mm_prefetch((char *)dists_out, _MM_HINT_T0);
    _mm_prefetch((char *)pq_ids, _MM_HINT_T0);
    _mm_prefetch((char *)(pq_ids + 64), _MM_HINT_T0);
    _mm_prefetch((char *)(pq_ids + 128), _MM_HINT_T0);
    memset(dists_out, 0, n_pts * sizeof(float));
    for (size_t chunk = 0; chunk < pq_nchunks; chunk++)
    {
        Poll();
        const float *chunk_dists = pq_dists + 256 * chunk;
        if (chunk < pq_nchunks - 1)
        {
            _mm_prefetch((char *)(chunk_dists + 256), _MM_HINT_T0);
        }
        for (size_t idx = 0; idx < n_pts; idx++)
        {
            uint8_t pq_centerid = pq_ids[pq_nchunks * idx + chunk];
            dists_out[idx] += chunk_dists[pq_centerid];
        }
    }
}
}

} // namespace pq_cpu_ms
