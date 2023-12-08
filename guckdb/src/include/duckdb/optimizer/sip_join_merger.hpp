#pragma once

#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

class SIPJoinMerger : public LogicalOperatorVisitor {
public:
    SIPJoinMerger (Binder &binder, ClientContext &context) : binder(binder), context(context) {
    }
	//! Search for joins to be rewritten
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

	//! Override this function to search for join operators
	void VisitOperator(LogicalOperator &op) override;

private:
    Binder &binder;
    ClientContext& context;

	void Merge(LogicalComparisonJoin &join);
};
}
