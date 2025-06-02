/*
	Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as
	published by the Free Software Foundation, either version 3 of the
	License, or (at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "sql_expression_utils.h"
#include "sql_expression_base.h"
#include "query_function.h"


SQLExprs::ExprRewriter::ExprRewriter(util::StackAllocator &alloc) :
		alloc_(alloc),
		localEntry_(alloc),
		entry_(&localEntry_) {
}

void SQLExprs::ExprRewriter::activateColumnMapping(ExprFactoryContext &cxt) {
	if (entry_->columnMapping_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const uint32_t inCount = cxt.getInputCount();

	entry_->columnMapping_ = true;
	entry_->columnMapUpdated_ = false;

	entry_->inputMap_.assign(inCount, -1);
	entry_->inputUsage_.assign(inCount, true);

	UsageList &columnUsage = entry_->columnUsage_;
	columnUsage.resize(inCount, Usage(alloc_));
	for (uint32_t i = 0; i < inCount; i++) {
		const uint32_t columnCount = cxt.getInputColumnCount(i);
		columnUsage[i].assign(columnCount, true);
	}

	entry_->idUsage_.assign(inCount, false);
	entry_->inputNullList_.assign(inCount, false);
}

void SQLExprs::ExprRewriter::clearInputUsage() {
	clearColumnUsage();

	ScopedEntry &entry = getColumnMappingEntry();

	{
		Usage &usage = entry.idUsage_;
		usage.assign(usage.size(), false);
	}

	{
		Usage &usage = entry.inputUsage_;
		usage.assign(usage.size(), false);
	}
}

void SQLExprs::ExprRewriter::addInputUsage(uint32_t input) {
	setInputUsage(input, true);
}

void SQLExprs::ExprRewriter::setInputUsage(uint32_t input, bool enabled) {
	ScopedEntry &entry = getColumnMappingEntry();

	Usage &usage = entry.inputUsage_;

	if (input >= usage.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	usage[input] = enabled;

	if (!enabled) {
		entry.idUsage_[input] = false;

		Usage &columnUsage = entry.columnUsage_[input];
		columnUsage.assign(columnUsage.size(), false);
	}
}

void SQLExprs::ExprRewriter::clearColumnUsage() {
	UsageList &usageList = getColumnMappingEntry().columnUsage_;
	for (UsageList::iterator it = usageList.begin();
			it != usageList.end(); ++it) {
		it->assign(it->size(), false);
	}
}

void SQLExprs::ExprRewriter::addColumnUsage(
		const Expression *expr, bool withId) {
	if (expr == NULL) {
		return;
	}
	addColumnUsage(*expr, withId);
}

void SQLExprs::ExprRewriter::addColumnUsage(
		const Expression &expr, bool withId) {
	const ExprCode &code = expr.getCode();

	if (code.getType() == SQLType::EXPR_COLUMN) {
		addColumnUsage(code.getInput(), code.getColumnPos());
	}
	else if (withId && code.getType() == SQLType::EXPR_ID) {
		setIdOfInput(code.getInput(), true);
	}
	else {
		for (Expression::Iterator it(expr); it.exists(); it.next()) {
			addColumnUsage(it.get(), withId);
		}
	}
}

void SQLExprs::ExprRewriter::addColumnUsage(uint32_t input, uint32_t column) {
	ScopedEntry &entry = getColumnMappingEntry();

	const Usage &inputUsage = entry.inputUsage_;
	UsageList &columnUsage = entry.columnUsage_;

	if (input >= columnUsage.size() || column >= columnUsage[input].size() ||
			!inputUsage[input]) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	columnUsage[input][column] = true;
}

void SQLExprs::ExprRewriter::addKeyColumnUsage(
		uint32_t input, const SQLValues::CompColumnList &keyList, bool front) {
	for (SQLValues::CompColumnList::const_iterator it = keyList.begin();
			it != keyList.end(); ++it) {
		addColumnUsage(input, it->getColumnPos(front));
	}
}

void SQLExprs::ExprRewriter::addInputColumnUsage(uint32_t input) {
	setInputColumnUsage(input, true);
}

void SQLExprs::ExprRewriter::setInputColumnUsage(
		uint32_t input, bool enabled) {
	ScopedEntry &entry = getColumnMappingEntry();

	const Usage &inputUsage = entry.inputUsage_;
	UsageList &columnUsage = entry.columnUsage_;

	if (input >= columnUsage.size() || !inputUsage[input]) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const size_t columnCount = columnUsage[input].size();
	columnUsage[input].assign(columnCount, enabled);
}

void SQLExprs::ExprRewriter::setIdOfInput(uint32_t input, bool enabled) {
	ScopedEntry &entry = getColumnMappingEntry();

	const Usage &inputUsage = entry.inputUsage_;
	Usage &usage = entry.idUsage_;

	if (input >= usage.size() || !inputUsage[input]) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	usage[input] = enabled;
}

void SQLExprs::ExprRewriter::setInputNull(uint32_t input, bool enabled) {
	ScopedEntry &entry = getColumnMappingEntry();

	const Usage &inputUsage = entry.inputUsage_;
	Usage &usage = entry.inputNullList_;

	if (input >= usage.size() || !inputUsage[input]) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	usage[input] = enabled;
}

void SQLExprs::ExprRewriter::setMappedInput(uint32_t src, uint32_t dest) {
	InputMap &map = getColumnMappingEntry().inputMap_;

	if (src >= map.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	map[src] = static_cast<int32_t>(dest);
}

void SQLExprs::ExprRewriter::setInputProjected(bool enabled) {
	getColumnMappingEntry().inputProjected_ = enabled;
}

void SQLExprs::ExprRewriter::setInputNullProjected(bool enabled) {
	getColumnMappingEntry().inputNullProjected_ = enabled;
}

void SQLExprs::ExprRewriter::setIdProjected(bool enabled) {
	getColumnMappingEntry().idProjected_ = enabled;
}

uint32_t SQLExprs::ExprRewriter::getMappedInput(
		uint32_t input, uint32_t column) const {
	return static_cast<uint32_t>(
			resolveColumnMapElement(input, column, false).first);
}

uint32_t SQLExprs::ExprRewriter::getMappedColumn(
		uint32_t input, uint32_t column) const {
	return static_cast<uint32_t>(
			resolveColumnMapElement(input, column, false).second);
}

uint32_t SQLExprs::ExprRewriter::getMappedIdInput(uint32_t input) const {
	return static_cast<uint32_t>(
			resolveColumnMapElement(input, 0, true).first);
}

uint32_t SQLExprs::ExprRewriter::getMappedIdColumn(uint32_t input) const {
	return static_cast<uint32_t>(
			resolveColumnMapElement(input, 0, true).second);
}

util::Vector<TupleColumnType> SQLExprs::ExprRewriter::createColumnTypeList(
		ExprFactoryContext &cxt, uint32_t input, bool unified) const {
	util::Vector<TupleColumnType> typeList(cxt.getAllocator());
	const uint32_t count = cxt.getInputColumnCount(input);
	for (uint32_t pos = 0; pos < count; pos++) {
		typeList.push_back(unified ?
				cxt.getUnifiedInputType(pos) : cxt.getInputType(input, pos));
	}
	return typeList;
}

util::Vector<SQLExprs::TupleColumn> SQLExprs::ExprRewriter::createColumnList(
		ExprFactoryContext &cxt, uint32_t input, bool unified) const {
	const util::Vector<TupleColumnType> typeList =
			createColumnTypeList(cxt, input, unified);

	util::Vector<TupleColumn> columnList(cxt.getAllocator());
	columnList.resize(typeList.size());

	TupleList::Info info;
	SQLValues::TypeUtils::setUpTupleInfo(info, &typeList[0], typeList.size());
	info.getColumns(&columnList[0], columnList.size());

	return columnList;
}

util::Vector<SQLValues::SummaryColumn>
SQLExprs::ExprRewriter::createSummaryColumnList(
		ExprFactoryContext &cxt, uint32_t input, bool unified, bool first,
		const SQLValues::CompColumnList *compColumnList,
		bool orderingRestricted) const {
	const util::Vector<TupleColumnType> typeList =
			createColumnTypeList(cxt, input, unified);

	SQLValues::ValueContext valueCxt(
			SQLValues::ValueContext::ofAllocator(cxt.getAllocator()));
	SQLValues::SummaryTupleSet tupleSet(valueCxt, NULL);

	tupleSet.addReaderColumnList(typeList);

	if (compColumnList != NULL) {
		tupleSet.addKeyList(*compColumnList, first, false);
		tupleSet.setOrderedDigestRestricted(orderingRestricted);
	}

	applySummaryTupleOptions(cxt, tupleSet);

	tupleSet.completeColumns();
	return tupleSet.getReaderColumnList();
}

void SQLExprs::ExprRewriter::applySummaryTupleOptions(
		ExprFactoryContext &cxt, SQLValues::SummaryTupleSet &tupleSet) {
	if (cxt.getDecrementalType() != ExprSpec::DECREMENTAL_NONE) {
		tupleSet.setValueDecremental(true);
	}
}

void SQLExprs::ExprRewriter::applyDecrementalType(
		ExprFactoryContext &cxt, const SQLExprs::ExprCode &code) {
	const uint32_t attributes = code.getAttributes();
	if ((attributes & ExprCode::ATTR_DECREMENTAL_FORWARD) != 0) {
		cxt.setDecrementalType(ExprSpec::DECREMENTAL_FORWARD);
	}
	else if ((attributes & ExprCode::ATTR_DECREMENTAL_BACKWARD) != 0) {
		cxt.setDecrementalType(ExprSpec::DECREMENTAL_BACKWARD);
	}
}

void SQLExprs::ExprRewriter::setCodeSetUpAlways(bool enabled) {
	entry_->codeSetUpAlways_ = enabled;
}

void SQLExprs::ExprRewriter::setMultiStageGrouping(bool enabled) {
	entry_->multiStageGrouping_ = enabled;
}

void SQLExprs::ExprRewriter::setInputMiddle(bool enabled) {
	assert(entry_->multiStageGrouping_ || !enabled);
	entry_->inputMiddle_ = enabled;
}

void SQLExprs::ExprRewriter::setOutputMiddle(bool enabled) {
	assert(entry_->multiStageGrouping_ || !enabled);
	entry_->outputMiddle_ = enabled;
}

SQLExprs::Expression& SQLExprs::ExprRewriter::rewrite(
		ExprFactoryContext &cxt, const Expression &src,
		Expression *destTop) const {
	const bool forProjection =
			(src.getCode().getType() == SQLType::EXPR_PROJECTION);
	const bool setUpRequired =
			isCodeSetUpRequired(src) ||
			isAggregationSetUpRequired(cxt, src, forProjection) ||
			isColumnMappingRequired();

	if (!setUpRequired) {
		return duplicate(cxt, src, destTop);
	}

	Expression *planningTop = (cxt.isPlanning() ? destTop : NULL);
	Expression *planningExpr;
	{
		ExprFactoryContext::Scope scope(cxt);
		cxt.setPlanning(true);
		planningExpr = &duplicate(cxt, src, planningTop);

		if (isCodeSetUpRequired(*planningExpr)) {
			size_t projDepth = 0;
			const size_t *projDepthPtr = (forProjection ? &projDepth : NULL);
			const uint32_t topAttributes =
					prepareTopAttributes(*planningExpr, forProjection, true);
			setUpCodes(
					cxt, *planningExpr, projDepthPtr, topAttributes, false);
		}

		if (isAggregationSetUpRequired(cxt, *planningExpr, forProjection)) {
			setUpAggregation(cxt, *planningExpr);
		}
	}

	remapColumn(cxt, *planningExpr);

	if (cxt.isPlanning()) {
		return *planningExpr;
	}
	else {
		return duplicate(cxt, *planningExpr, destTop);
	}
}

SQLExprs::Expression& SQLExprs::ExprRewriter::rewritePredicate(
		ExprFactoryContext &cxt, const Expression *src) const {
	if (src == NULL) {
		const Expression &base = createConstExpr(
				cxt, SQLValues::ValueUtils::toAnyByBool(true));
		return rewrite(cxt, base, NULL);
	}
	return rewrite(cxt, *src, NULL);
}

SQLValues::CompColumnList& SQLExprs::ExprRewriter::rewriteCompColumnList(
		ExprFactoryContext &cxt, const SQLValues::CompColumnList &src,
		bool unified, const uint32_t *inputRef,
		bool orderingRestricted) const {
	const uint32_t firstInput = (inputRef == NULL ? 0 : *inputRef);
	const uint32_t secondInput = (inputRef == NULL ?
		(unified || cxt.getInputCount() <= 1 ? 0 : 1) : *inputRef);

	util::Vector<SummaryColumn> columnList1 = createSummaryColumnList(
			cxt, firstInput, unified, true, &src, orderingRestricted);
	util::Vector<SummaryColumn> columnList2 = createSummaryColumnList(
			cxt, secondInput, unified, false, &src, orderingRestricted);

	util::StackAllocator &alloc = cxt.getAllocator();
	SQLValues::CompColumnList &dest =
			*(ALLOC_NEW(alloc) SQLValues::CompColumnList(alloc));
	for (SQLValues::CompColumnList::const_iterator it = src.begin();
			it != src.end(); ++it) {
		SQLValues::CompColumn column = *it;
		assert(column.isColumnPosAssigned(true));
		const bool second = column.isColumnPosAssigned(false);

		const uint32_t basePos1 = column.getColumnPos(true);
		const uint32_t basePos2 = column.getColumnPos(!second);

		column.setSummaryColumn(columnList1[basePos1], true);
		column.setSummaryColumn(columnList2[basePos2], false);

		const bool anyAsNull = true;
		TupleColumnType type = SQLValues::TypeUtils::findPromotionType(
				column.getTupleColumn1().getType(),
				column.getTupleColumn2().getType(),
				anyAsNull);
		if (SQLValues::TypeUtils::isNull(type)) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION,"");
		}
		column.setType(type);

		uint32_t pos1;
		uint32_t pos2;
		if (entry_->columnMapping_) {
			pos1 = getMappedColumn(firstInput, basePos1);
			pos2 = getMappedColumn(secondInput, basePos2);
		}
		else {
			pos1 = basePos1;
			pos2 = basePos2;
		}
		column.setColumnPos(pos1, true);
		column.setColumnPos(pos2, false);

		dest.push_back(column);
	}

	return dest;
}

void SQLExprs::ExprRewriter::popLastDistinctAggregationOutput(
		ExprFactoryContext &cxt, Expression &destTop) const {
	Expression *&srcExpr = entry_->distinctAggrsExpr_;
	if (srcExpr == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	Expression::ModIterator destIt(destTop);
	assert(!destIt.exists());

	Expression::ModIterator srcIt(*srcExpr);
	while (srcIt.exists()) {
		Expression &subExpr = srcIt.get();
		srcIt.remove();
		destIt.append(subExpr);
	}

	destTop.getPlanningCode().setAttributes(
			destTop.getPlanningCode().getAttributes() |
			srcExpr->getCode().getAttributes());
	srcExpr = NULL;

	if (!isFinishAggregationArranging(cxt)) {
		remapColumn(cxt, destTop);
	}
}

void SQLExprs::ExprRewriter::remapColumn(
		ExprFactoryContext &cxt, Expression &expr) const {
	if (!isColumnMappingRequired()) {
		return;
	}

	if (!cxt.isPlanning()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	remapColumnSub(cxt, expr);
}

void SQLExprs::ExprRewriter::normalizeCompColumnList(
		SQLValues::CompColumnList &list, util::Set<uint32_t> &posSet) {
	assert(posSet.empty());
	for (SQLValues::CompColumnList::iterator it = list.begin();
			it != list.end();) {
		assert(it->isColumnPosAssigned(false));
		const uint32_t pos = it->getColumnPos(true);
		const bool duplicate = !posSet.insert(pos).second;
		if (duplicate) {
			it = list.erase(it);
		}
		else {
			++it;
		}
	}
}

SQLValues::CompColumnList& SQLExprs::ExprRewriter::remapCompColumnList(
		ExprFactoryContext &cxt, const SQLValues::CompColumnList &src,
		uint32_t input, bool front, bool keyOnly,
		const util::Set<uint32_t> *keyPosSet, bool inputMapping) const {
	assert(!keyOnly || (keyPosSet != NULL && keyPosSet->size() == src.size()));
	static_cast<void>(keyPosSet);

	util::StackAllocator &alloc = cxt.getAllocator();
	SQLValues::CompColumnList &dest =
			*(ALLOC_NEW(alloc) SQLValues::CompColumnList(alloc));

	bool destFront = front;
	if (inputMapping) {
		const uint32_t destInput = resolveMappedInput(input);
		assert(destInput == 0 || destInput == 1);
		destFront = (destInput == 0);
	}

	uint32_t keyOnlyPos = 0;
	if (isIdUsed(input)) {
		SQLValues::CompColumn key;
		key.setColumnPos(getMappedIdColumn(input), destFront);
		key.setOrdering(true);
		dest.push_back(key);
		keyOnlyPos++;
	}

	for (SQLValues::CompColumnList::const_iterator it = src.begin();
			it != src.end(); ++it) {
		const uint32_t srcPos = it->getColumnPos(front);
		uint32_t pos;
		if (keyOnly) {
			assert(keyPosSet->find(srcPos) != keyPosSet->end());
			pos = keyOnlyPos;
			keyOnlyPos++;
		}
		else {
			pos = getMappedColumn(input, srcPos);
		}

		SQLValues::CompColumn key;
		key.setColumnPos(pos, destFront);
		key.setOrdering(it->isOrdering());
		dest.push_back(key);
	}

	return dest;
}

void SQLExprs::ExprRewriter::createIdenticalProjection(
		ExprFactoryContext &cxt, bool inputUnified, uint32_t input,
		Expression &dest) {
	Expression::ModIterator it(dest);

	const uint32_t count = cxt.getInputColumnCount(input);
	for (uint32_t pos = 0; pos < count; pos++) {
		it.append(createTypedColumnExpr(cxt, inputUnified, input, pos));
	}
}

void SQLExprs::ExprRewriter::createEmptyRefProjection(
		ExprFactoryContext &cxt, bool inputUnified, uint32_t input,
		uint32_t startColumn, const util::Set<uint32_t> *keySet,
		Expression &dest) {
	Expression::ModIterator it(dest);

	const uint32_t count = cxt.getInputColumnCount(input);
	for (uint32_t pos = 0; pos < count; pos++) {
		const bool empty = (pos >= startColumn && (keySet == NULL ||
				keySet->find(pos) == keySet->end()));
		it.append((empty ?
				createEmptyRefConstExpr(cxt, inputUnified, input, pos) :
				createTypedColumnExpr(cxt, inputUnified, input, pos)));
	}
}

void SQLExprs::ExprRewriter::createProjectionByUsage(
		ExprFactoryContext &cxt, bool inputUnified, Expression &dest) const {
	Expression::ModIterator it(dest);

	const ExprFactory &factory = cxt.getFactory();

	const uint32_t inCount = cxt.getInputCount();
	for (uint32_t i = 0; i < inCount; i++) {
		const uint32_t columnCount = cxt.getInputColumnCount(i);
		bool forId = true;
		for (uint32_t pos = 0;;) {
			ExprCode code;
			const bool throwOnError = false;
			if (assignRemappedCode(
					cxt, i, pos, forId, inputUnified, code, throwOnError)) {
				it.append(factory.create(cxt, code));
			}

			if (forId) {
				forId = false;
				continue;
			}
			else if (++pos >= columnCount) {
				break;
			}
		}
	}
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createConstExpr(
		ExprFactoryContext &cxt,
		const TupleValue &value, TupleColumnType type) {
	ExprFactoryContext::Scope scope(cxt);
	cxt.setPlanning(true);

	SQLValues::ValueContext valueCxt(
			SQLValues::ValueContext::ofAllocator(cxt.getAllocator()));

	ExprCode code;
	code.setType(SQLType::EXPR_CONSTANT);
	code.setValue(valueCxt, value);
	code.setColumnType(type);

	return cxt.getFactory().create(cxt, code);
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createCastExpr(
		ExprFactoryContext &cxt, Expression &baseExpr, TupleColumnType type) {
	ExprFactoryContext::Scope scope(cxt);
	cxt.setPlanning(true);

	Expression *castExpr;
	{
		ExprCode code;
		code.setType(SQLType::OP_CAST);
		code.setColumnType(type);
		castExpr = &cxt.getFactory().create(cxt, code);
	}

	Expression *typeExpr;
	{
		ExprCode code;
		code.setType(SQLType::EXPR_TYPE);
		code.setColumnType(type);
		typeExpr = &cxt.getFactory().create(cxt, code);
	}

	Expression::ModIterator it(*castExpr);
	it.append(baseExpr);
	it.append(*typeExpr);

	return *castExpr;
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createColumnExpr(
		ExprFactoryContext &cxt, uint32_t input, uint32_t column) {
	ExprFactoryContext::Scope scope(cxt);
	cxt.setPlanning(true);

	ExprCode code;
	code.setType(SQLType::EXPR_COLUMN);
	code.setInput(input);
	code.setColumnPos(column);

	return cxt.getFactory().create(cxt, code);
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createTypedColumnExpr(
		ExprFactoryContext &cxt, bool inputUnified, uint32_t input,
		uint32_t column) {
	Expression &expr = createColumnExpr(cxt, input, column);
	ExprCode &code = expr.getPlanningCode();
	if (inputUnified) {
		code.setAttributes(ExprCode::ATTR_COLUMN_UNIFIED);
	}
	code.setColumnType(getRefColumnType(cxt, inputUnified, input, column));
	return expr;
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createTypedColumnExprBy(
		ExprFactoryContext &cxt, uint32_t input, uint32_t column,
		TupleColumnType type) {
	Expression &expr = createColumnExpr(cxt, input, column);
	ExprCode &code = expr.getPlanningCode();
	code.setColumnType(type);
	return expr;
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createTypedIdExpr(
		ExprFactoryContext &cxt, uint32_t input) {
	Expression &expr = cxt.getFactory().create(cxt, SQLType::EXPR_ID);

	ExprCode &code = expr.getPlanningCode();
	code.setInput(input);
	code.setColumnType(getIdColumnType(cxt.getFactory()));
	code.setAttributes(ExprCode::ATTR_ASSIGNED);

	return expr;
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createEmptyRefConstExpr(
		ExprFactoryContext &cxt, bool inputUnified, uint32_t input,
		uint32_t column) {
	const TupleColumnType type =
			getRefColumnType(cxt, inputUnified, input, column);
	return createEmptyConstExpr(cxt, type);
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createEmptyConstExpr(
		ExprFactoryContext &cxt, TupleColumnType type) {
	assert(!SQLValues::TypeUtils::isNull(type));

	SQLValues::ValueContext valueCxt(
			SQLValues::ValueContext::ofAllocator(cxt.getAllocator()));
	const TupleValue &value =
			SQLValues::ValueUtils::createEmptyValue(valueCxt, type);

	return createConstExpr(cxt, value, type);
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createIdRefColumnExpr(
		ExprFactoryContext &cxt, uint32_t input, uint32_t column) {
	ExprFactoryContext::Scope scope(cxt);
	cxt.setPlanning(true);

	ExprCode code;
	code.setType(SQLType::EXPR_COLUMN);
	code.setInput(input);
	code.setColumnPos(column);
	code.setColumnType(getIdColumnType(cxt.getFactory()));

	return cxt.getFactory().create(cxt, code);
}

TupleColumnType SQLExprs::ExprRewriter::getRefColumnType(
		ExprFactoryContext &cxt, bool inputUnified, uint32_t input,
		uint32_t column) {
	return (inputUnified ?
			cxt.getUnifiedInputType(column) : cxt.getInputType(input, column));
}

TupleColumnType SQLExprs::ExprRewriter::getIdColumnType(
		const ExprFactory &factory) {
	const TupleColumnType type = factory.getSpec(SQLType::EXPR_ID).outType_;
	assert(!SQLValues::TypeUtils::isNull(type));
	return type;
}

const SQLExprs::Expression* SQLExprs::ExprRewriter::findPredicateBySelection(
		const Expression &selectExpr) {
	assert(selectExpr.getCode().getType() == SQLType::EXPR_SELECTION);
	const Expression &production = selectExpr.child();

	assert(production.getCode().getType() == SQLType::EXPR_PRODUCTION);
	return production.findChild();
}

const SQLExprs::Expression& SQLExprs::ExprRewriter::getProjectionBySelection(
		const Expression &selectExpr) {
	assert(selectExpr.getCode().getType() == SQLType::EXPR_SELECTION);
	const Expression &projection = selectExpr.child().next();

	assert(projection.getCode().getType() == SQLType::EXPR_PROJECTION);
	return projection;
}

SQLExprs::Expression& SQLExprs::ExprRewriter::duplicate(
		ExprFactoryContext &cxt, const Expression &src,
		Expression *destTop) {
	ExprFactoryContext::Scope scope(cxt);
	cxt.setBaseExpression(&src);

	Expression &dest = (destTop == NULL ?
			cxt.getFactory().create(cxt, src.getCode()) : *destTop);

	if (cxt.isArgChecking()) {
		cxt.setArgChecking(false);
	}

	if (destTop != NULL && destTop->isPlannable()) {
		destTop->getPlanningCode().setAttributes(
				(destTop->getCode().getAttributes() |
				src.getCode().getAttributes()));
	}

	Expression::ModIterator destIt(dest);
	for (Expression::Iterator it(src); it.exists(); it.next()) {
		destIt.append(duplicate(cxt, it.get(), NULL));
	}

	return dest;
}

SQLExprs::Expression* SQLExprs::ExprRewriter::compColumnListToPredicate(
		ExprFactoryContext &cxt, const SQLValues::CompColumnList &src) {
	util::Vector<Expression*> list(cxt.getAllocator());

	for (SQLValues::CompColumnList::const_iterator it = src.begin();
			it != src.end(); ++it) {
		const ExprType type = SQLExprs::ExprTypeUtils::getCompColumnOp(*it);

		Expression &expr = cxt.getFactory().create(cxt, type);
		expr.addChild(&createColumnExpr(cxt, 0, it->getColumnPos(true)));
		expr.addChild(&createColumnExpr(cxt, 1, it->getColumnPos(false)));
		list.push_back(&expr);
	}

	return createExprTree(cxt, SQLType::EXPR_AND, list);
}

SQLExprs::Expression*
SQLExprs::ExprRewriter::compColumnListToKeyFilterPredicate(
		ExprFactoryContext &cxt, const SQLValues::CompColumnList &src,
		bool first, const Expression *otherPred) {
	Expression *pred = compColumnListToKeyFilterPredicate(cxt, src, first);
	if (otherPred != NULL) {
		ExprFactoryContext::Scope scope(cxt);
		cxt.setAggregationPhase(true, SQLType::AGG_PHASE_ALL_PIPE);
		pred = &mergePredicate(cxt, pred, &rewrite(cxt, *otherPred, NULL));
	}
	return pred;
}

SQLExprs::Expression*
SQLExprs::ExprRewriter::compColumnListToKeyFilterPredicate(
		ExprFactoryContext &cxt, const SQLValues::CompColumnList &src,
		bool first) {
	const uint32_t input = (first ? 0 : 1);
	util::Vector<Expression*> condList(cxt.getAllocator());

	for (SQLValues::CompColumnList::const_iterator it = src.begin();
			it != src.end(); ++it) {
		const uint32_t pos = it->getColumnPos(first);
		const TupleColumnType type = cxt.getInputType(input, pos);
		if (!SQLValues::TypeUtils::isNullable(type) &&
				!SQLValues::TypeUtils::isAny(type)) {
			continue;
		}

		Expression &expr = cxt.getFactory().create(cxt, SQLType::OP_IS_NOT_NULL);
		expr.addChild(&createColumnExpr(cxt, input, pos));
		expr.getPlanningCode().setColumnType(TupleTypes::TYPE_BOOL);
		expr.getPlanningCode().setAttributes(ExprCode::ATTR_ASSIGNED);
		condList.push_back(&expr);
	}

	return createExprTree(cxt, SQLType::EXPR_AND, condList);
}

SQLExprs::Expression& SQLExprs::ExprRewriter::replaceColumnToConstExpr(
		ExprFactoryContext &cxt, Expression &src) {
	const ExprCode &code = src.getCode();
	if (code.getType() == SQLType::EXPR_COLUMN) {
		return createEmptyConstExpr(cxt, code.getColumnType());
	}

	for (Expression::ModIterator it(src); it.exists(); it.next()) {
		Expression &sub = it.get();
		it.remove();
		it.insert(replaceColumnToConstExpr(cxt, sub));
	}

	return src;
}

SQLExprs::Expression& SQLExprs::ExprRewriter::retainSingleInputPredicate(
		ExprFactoryContext &cxt, Expression &expr, uint32_t input,
		bool negative) {
	const ExprType logicalOp =
			ExprTypeUtils::getLogicalOp(expr.getCode().getType(), negative);

	bool subNegative;
	do {
		if (logicalOp == SQLType::EXPR_AND || logicalOp == SQLType::EXPR_OR) {
			subNegative = negative;
			break;
		}
		else if (logicalOp == SQLType::OP_NOT) {
			subNegative = !negative;
			break;
		}
		else if (checkSingleInput(expr, input)) {
			return expr;
		}
		return createConstExpr(
				cxt, SQLValues::ValueUtils::toAnyByBool(!negative));
	}
	while (false);

	for (Expression::ModIterator it(expr); it.exists(); it.next()) {
		Expression &sub = it.get();
		it.remove();
		it.insert(retainSingleInputPredicate(cxt, sub, input, subNegative));
	}
	return expr;
}

bool SQLExprs::ExprRewriter::checkSingleInput(
		const Expression &expr, uint32_t input) {
	const ExprCode &code = expr.getCode();
	const ExprType type = code.getType();

	if (type == SQLType::EXPR_COLUMN) {
		return (code.getInput() == input);
	}

	for (Expression::Iterator it(expr); it.exists(); it.next()) {
		if (!checkSingleInput(it.get(), input)) {
			return false;
		}
	}

	return true;
}

SQLExprs::Expression* SQLExprs::ExprRewriter::createExprTree(
		ExprFactoryContext &cxt, ExprType type,
		util::Vector<Expression*> &list) {
	while (list.size() > 1) {
		util::Vector<Expression*>::const_iterator srcIt = list.begin();
		util::Vector<Expression*>::iterator destIt = list.begin();
		while (srcIt != list.end()) {
			Expression *arg1 = *srcIt;
			if (++srcIt == list.end()) {
				*destIt = arg1;
			}
			else {
				Expression &expr = cxt.getFactory().create(cxt, type);
				expr.addChild(arg1);

				Expression *arg2 = *srcIt;
				expr.addChild(arg2);
				++srcIt;

				*destIt = &expr;
			}
			++destIt;
		}
		list.erase(destIt, list.end());
	}

	if (list.empty()) {
		return NULL;
	}
	else {
		return list.front();
	}
}

SQLExprs::Expression& SQLExprs::ExprRewriter::mergePredicate(
		ExprFactoryContext &cxt,
		Expression *leftExpr, Expression *rightExpr) {
	const bool leftTrue = (leftExpr == NULL || isTruePredicate(*leftExpr));
	const bool rightTrue = (rightExpr == NULL || isTruePredicate(*rightExpr));

	if (leftTrue || rightTrue) {
		if (leftTrue && rightTrue) {
			return createTruePredicate(cxt);
		}
		else {
			return (leftTrue ? *rightExpr : *leftExpr);
		}
	}

	Expression &expr = cxt.getFactory().create(cxt, SQLType::EXPR_AND);
	Expression::ModIterator it(expr);
	it.append(*leftExpr);
	it.append(*rightExpr);
	return expr;
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createTruePredicate(
		ExprFactoryContext &cxt) {
	return createConstExpr(cxt, SQLValues::ValueUtils::toAnyByBool(true));
}

bool SQLExprs::ExprRewriter::isTruePredicate(const Expression &expr) {
	const ExprCode &code = expr.getCode();
	return (code.getType() == SQLType::EXPR_CONSTANT &&
			SQLValues::ValueUtils::toBool(code.getValue()));
}

bool SQLExprs::ExprRewriter::isConstEvaluable(
			const ExprFactory &factory, ExprType type) {
	if (ExprTypeUtils::isAggregation(type)) {
		return false;
	}

	const ExprSpec &spec = factory.getSpec(type);
	if ((spec.flags_ & ExprSpec::FLAG_DYNAMIC) != 0) {
		return false;
	}

	return true;
}

std::pair<bool, bool> SQLExprs::ExprRewriter::isConstWithPlaceholder(
			const ExprFactory &factory, const Expression &expr) {
	const ExprType type = expr.getCode().getType();
	if (type == SQLType::EXPR_PLACEHOLDER) {
		return std::make_pair(true, true);
	}
	else if (!isConstEvaluable(factory, type)) {
		return std::make_pair(false, true);
	}

	bool found = false;
	for (Expression::Iterator it(expr); it.exists(); it.next()) {
		const std::pair<bool, bool> &ret =
				isConstWithPlaceholder(factory, it.get());
		if (!ret.first && ret.second) {
			return ret;
		}
		found |= ret.first;
	}

	return found ? std::make_pair(true, true) : std::make_pair(false, false);
}

TupleValue SQLExprs::ExprRewriter::evalConstExpr(
		ExprContext &cxt, const Expression &expr) {
	SQLValues::VarContext::Scope varScope(
			cxt.getValueContext().getVarContext());
	TupleValue value = expr.eval(cxt);
	varScope.moveValueToParent(value);

	return value;
}

bool SQLExprs::ExprRewriter::checkArgCount(
		const ExprFactory &factory, ExprType exprType, size_t argCount,
		AggregationPhase phase, bool throwOnError) {
	const ExprSpec &spec = factory.getSpec(exprType);
	return checkArgCount(spec, exprType, argCount, phase, throwOnError);
}

bool SQLExprs::ExprRewriter::checkArgCount(
		const ExprSpec &spec, ExprType exprType, size_t argCount,
		AggregationPhase phase, bool throwOnError) {
	TypeResolver resolver(spec, phase, false);

	const int32_t min = resolver.getMinArgCount();
	if (argCount < static_cast<size_t>(min)) {
		if (!throwOnError) {
			return false;
		}
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION,
				"Too less args (type=" << SQLType::Coder()(exprType, "") <<
				", specified=" << argCount <<
				", min=" << min << ")");
	}

	const int32_t max = resolver.getMaxArgCount();
	if (max >= 0 && argCount > static_cast<size_t>(max)) {
		if (!throwOnError) {
			return false;
		}
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION,
				"Too many args (type=" << SQLType::Coder()(exprType, "") <<
				", specified=" << argCount <<
				", max=" << max << ")");
	}

	return true;
}

uint32_t SQLExprs::ExprRewriter::getResultCount(
		const ExprFactory &factory, ExprType exprType,
		AggregationPhase phase) {
	return getResultCount(factory.getSpec(exprType), phase);
}

uint32_t SQLExprs::ExprRewriter::getResultCount(
		const ExprSpec &spec, AggregationPhase phase) {
	TypeResolver resolver(spec, phase, false);
	return resolver.getResultCount();
}

void SQLExprs::ExprRewriter::removeUnificationAttributes(Expression &expr) {
	ExprCode &code = expr.getPlanningCode();
	code.setAttributes(
			code.getAttributes() &
			~static_cast<uint32_t>(ExprCode::ATTR_COLUMN_UNIFIED));

	for (Expression::ModIterator it(expr); it.exists(); it.next()) {
		removeUnificationAttributes(it.get());
	}
}

bool SQLExprs::ExprRewriter::findDistinctAggregation(
		const ExprFactory &factory, const Expression &expr) {
	if (isDistinctAggregation(factory, expr.getCode().getType())) {
		return true;
	}

	for (Expression::Iterator it(expr); it.exists(); it.next()) {
		if (findDistinctAggregation(factory, it.get())) {
			return true;
		}
	}
	return false;
}

bool SQLExprs::ExprRewriter::isDistinctAggregation(
		const ExprFactory &factory, ExprType type) {
	return isDistinctAggregation(factory.getSpec(type), type);
}

bool SQLExprs::ExprRewriter::isDistinctAggregation(
		const ExprSpec &spec, const ExprType type) {
	return (ExprTypeUtils::isAggregation(type) &&
			(spec.flags_ & ExprSpec::FLAG_DISTINCT) != 0);
}

bool SQLExprs::ExprRewriter::isWindowExpr(
		const ExprSpec &spec, bool withSpecialArgs) {
	if ((spec.flags_ & (
			ExprSpec::FLAG_WINDOW |
			ExprSpec::FLAG_WINDOW_ONLY |
			ExprSpec::FLAG_PSEUDO_WINDOW)) == 0) {
		return false;
	}

	if (!withSpecialArgs) {
		return true;
	}

	if ((spec.flags_ & ExprSpec::FLAG_WINDOW_VALUE_COUNTING) != 0) {
		return true;
	}

	if (findWindowPosArgIndex(spec, NULL)) {
		return true;
	}

	return false;
}

bool SQLExprs::ExprRewriter::findWindowPosArgIndex(
		const ExprSpec &spec, uint32_t *index) {
	if (index == NULL) {
		uint32_t ret;
		return findWindowPosArgIndex(spec, &ret);
	}

	*index = std::numeric_limits<uint32_t>::max();

	for (uint32_t i = 0; i < ExprSpec::IN_LIST_SIZE; i++) {
		const ExprSpec::In &in = spec.inList_[i];
		if (SQLValues::TypeUtils::isNull(in.typeList_[0])) {
			break;
		}

		if ((in.flags_ & (
				ExprSpec::FLAG_WINDOW_POS_BEFORE |
				ExprSpec::FLAG_WINDOW_POS_AFTER)) != 0) {
			*index = i;
			return true;
		}
	}

	return false;
}

void SQLExprs::ExprRewriter::getDistinctExprList(
		const ExprFactory &factory, const Expression &expr,
		util::Vector<const Expression*> &exprList) {

	if (isDistinctAggregation(factory, expr.getCode().getType())) {
		exprList.push_back(&expr);
		return;
	}

	for (Expression::Iterator it(expr); it.exists(); it.next()) {
		getDistinctExprList(factory, it.get(), exprList);
	}
}

SQLExprs::Expression& SQLExprs::ExprRewriter::toNonDistinctExpr(
		ExprFactoryContext &cxt, const Expression &src, uint32_t input,
		uint32_t *refColumnPos) {
	assert(refColumnPos != NULL);

	ExprType destExprType;
	const TypeResolverResult &retInfo = resolveNonDistinctExprColumnTypes(
			cxt.getFactory(), src, &destExprType);

	assert(cxt.isPlanning());
	ExprRewriter rewriter(cxt.getAllocator());
	Expression &dest = rewriter.rewrite(cxt, src, NULL);

	dest.getPlanningCode().setType(destExprType);
	dest.getPlanningCode().setColumnType(retInfo.typeList_[0]);

	for (Expression::ModIterator it(dest); it.exists(); it.next()) {
		const Expression &subSrc = it.get();

		Expression &subDest = createColumnExpr(cxt, input, *refColumnPos);
		subDest.getPlanningCode().setColumnType(
				subSrc.getCode().getColumnType());
		it.remove();
		it.insert(subDest);

		(*refColumnPos)++;
	}

	return dest;
}

void SQLExprs::ExprRewriter::addDistinctRefColumnExprs(
		ExprFactoryContext &cxt, const Expression &src, bool forAdvance,
		bool refEmpty, uint32_t input, uint32_t *refColumnPos,
		Expression::ModIterator &destIt) {
	if (forAdvance) {
		for (Expression::Iterator it(src); it.exists(); it.next()) {
			addDistinctRefColumnExprs(
					cxt, it.get(), false, refEmpty, input, refColumnPos,
					destIt);
		}
	}
	else {
		const ExprCode &srcCode = src.getCode();
		const uint32_t pos = (refColumnPos == NULL ?
				srcCode.getColumnPos() : *refColumnPos);

		assert(refColumnPos != NULL ||
				srcCode.getType() == SQLType::EXPR_COLUMN);

		const TupleColumnType type = srcCode.getColumnType();
		Expression *refExpr;
		if (refEmpty) {
			refExpr = &createEmptyConstExpr(cxt, type);
		}
		else {
			refExpr = &createColumnExpr(cxt, input, pos);
			refExpr->getPlanningCode().setColumnType(type);
		}
		destIt.append(*refExpr);

		if (refColumnPos != NULL) {
			(*refColumnPos)++;
		}
	}
}

void SQLExprs::ExprRewriter::replaceDistinctExprsToRef(
		ExprFactoryContext &cxt, Expression &expr, bool forAdvance,
		int32_t emptySide, const util::Set<uint32_t> *keySet,
		uint32_t *restDistinctCount, uint32_t *refColumnPos) {

	if (restDistinctCount == NULL || refColumnPos == NULL) {
		util::Vector<const Expression*> exprList(cxt.getAllocator());
		getDistinctExprList(cxt.getFactory(), expr, exprList);
		uint32_t restDistinctCountBase = static_cast<uint32_t>(exprList.size());

		uint32_t refColumnPosBase = cxt.getInputColumnCount(0);
		replaceDistinctExprsToRef(
				cxt, expr, forAdvance, emptySide, keySet,
				&restDistinctCountBase, &refColumnPosBase);

		assert(restDistinctCountBase == 0);
		return;
	}

	assert(!isDistinctAggregation(cxt.getFactory(), expr.getCode().getType()));

	for (Expression::ModIterator it(expr); it.exists();) {
		Expression &subExpr = it.get();
		const ExprType subType = subExpr.getCode().getType();
		if (isDistinctAggregation(cxt.getFactory(), subType)) {
			it.remove();

			assert(*restDistinctCount > 0);
			const bool merged = (*restDistinctCount > 1);
			const bool refEmpty = (emptySide >= 0 ?
					(emptySide == 0 ? merged : !merged) : false);

			const uint32_t input = (merged ? 0 : 1);
			const uint32_t refColumnPosBase = (merged ? *refColumnPos : 1);
			uint32_t refColumnPosSub = refColumnPosBase;
			addDistinctRefColumnExprs(
					cxt, subExpr, forAdvance, refEmpty, input,
					&refColumnPosSub, it);

			(*refColumnPos) += (refColumnPosSub - refColumnPosBase);
			(*restDistinctCount)--;
		}
		else if (subType == SQLType::EXPR_AGG_FOLLOWING) {
			it.remove();
		}
		else if (subType == SQLType::EXPR_COLUMN && emptySide == 0 &&
				(keySet == NULL || keySet->find(
						subExpr.getCode().getColumnPos()) == keySet->end())) {
			it.remove();
			it.append(createEmptyConstExpr(
					cxt, subExpr.getCode().getColumnType()));
		}
		else {
			replaceDistinctExprsToRef(
					cxt, subExpr, forAdvance, emptySide, keySet,
					restDistinctCount, refColumnPos);
			it.next();
		}
	}
}

bool SQLExprs::ExprRewriter::findVarGenerativeExpr(const Expression &expr) {
	const ExprCode &code = expr.getCode();

	const TupleColumnType type = code.getColumnType();
	if (!SQLValues::TypeUtils::isNull(type) &&
			!SQLValues::TypeUtils::isAny(type) &&
			!SQLValues::TypeUtils::isNormalFixed(type) &&
			!ExprTypeUtils::isVarNonGenerative(
					code.getType(),
					SQLValues::TypeUtils::isLob(type),
					SQLValues::TypeUtils::isLargeFixed(type))) {
		return true;
	}

	for (Expression::Iterator it(expr); it.exists(); it.next()) {
		if (findVarGenerativeExpr(it.get())) {
			return true;
		}
	}

	return false;
}

SQLValues::CompColumnList& SQLExprs::ExprRewriter::reduceDuplicateCompColumns(
		util::StackAllocator &alloc,
		const SQLValues::CompColumnList &srcList) {
	util::Set<uint32_t> columnSet(alloc);

	SQLValues::CompColumnList &destList =
			*(ALLOC_NEW(alloc) SQLValues::CompColumnList(alloc));
	for (SQLValues::CompColumnList::const_iterator it = srcList.begin();
			it != srcList.end(); ++it) {
		if (columnSet.insert(it->getColumnPos(true)).second) {
			destList.push_back(*it);
		}
	}

	return destList;
}

util::Set<uint32_t>& SQLExprs::ExprRewriter::compColumnListToSet(
		util::StackAllocator &alloc, const SQLValues::CompColumnList &srcList) {
	util::Set<uint32_t> &dest = *(ALLOC_NEW(alloc) util::Set<uint32_t>(alloc));

	for (SQLValues::CompColumnList::const_iterator it = srcList.begin();
			it != srcList.end(); ++it) {
		dest.insert(it->getColumnPos(true));
	}

	return dest;
}

bool SQLExprs::ExprRewriter::predicateToExtContainerName(
		SQLValues::ValueContext &cxt, const ExprFactory &factory,
		const Expression *pred, uint32_t dbNameColumn,
		uint32_t containerNameColumn, uint32_t partitionNameColumn,
		TupleValue *dbName, TupleValue &containerName,
		std::pair<TupleValue, TupleValue> *containerNameElems,
		bool &placeholderAffected) {
	placeholderAffected = false;

	if (pred == NULL) {
		return false;
	}

	const ExprType predType = pred->getCode().getType();
	if (predType == SQLType::EXPR_AND) {
		const Expression *expr = pred->findChild();
		while (expr != NULL) {
			bool placeholderAffectedLocal;
			const bool resolved = predicateToExtContainerName(
					cxt, factory, expr,
					dbNameColumn, containerNameColumn, partitionNameColumn,
					dbName, containerName, containerNameElems,
					placeholderAffectedLocal);

			placeholderAffected |= placeholderAffectedLocal;
			if (resolved) {
				return true;
			}

			expr = expr->findNext();
		}
		return false;
	}
	else if (
			predType == SQLType::OP_EQ ||
			predType == SQLType::OP_IS ||
			predType == SQLType::FUNC_LIKE) {
		const bool forIsNull = (predType == SQLType::OP_IS);
		const bool forLike = (predType == SQLType::FUNC_LIKE);

		const Expression *expr1 = pred->findChild();
		const Expression *expr2 = (expr1 == NULL ? NULL : expr1->findNext());
		const Expression *expr3 = (expr2 == NULL ? NULL : expr2->findNext());

		if (expr1->getCode().getType() == SQLType::EXPR_CONSTANT || forLike) {
			std::swap(expr1, expr2);
		}

		if (isConstWithPlaceholder(factory, *expr2).first ||
				(expr3 != NULL &&
				isConstWithPlaceholder(factory, *expr3).first)) {
			placeholderAffected = true;
		}

		if (expr2->getCode().getType() != SQLType::EXPR_CONSTANT) {
			return false;
		}
		else if (forIsNull &&
				!SQLValues::ValueUtils::isNull(expr2->getCode().getValue())) {
			return false;
		}

		if (expr3 != NULL && (!forLike ||
				expr3->getCode().getType() != SQLType::EXPR_CONSTANT)) {
			return false;
		}

		const Expression &metaExpr =
				(expr1->getCode().getType() == SQLType::FUNC_UPPER &&
				expr1->findChild() != NULL ? expr1->child() : *expr1);
		if (metaExpr.getCode().getType() != SQLType::EXPR_COLUMN) {
			return false;
		}
		const uint32_t column = metaExpr.getCode().getColumnPos();

		bool forContainer = false;
		TupleValue *valueRef;
		if (column == dbNameColumn) {
			valueRef = dbName;
		}
		else if (column == containerNameColumn) {
			forContainer = true;
			valueRef = (containerNameElems == NULL ?
					&containerName : &containerNameElems->first);
		}
		else if (column == partitionNameColumn) {
			forContainer = true;
			valueRef = (containerNameElems == NULL ?
					NULL : &containerNameElems->second);
		}
		else {
			return false;
		}

		if (valueRef == NULL || !SQLValues::ValueUtils::isNull(*valueRef)) {
			return false;
		}

		const TupleValue &constValue2 = expr2->getCode().getValue();
		if (constValue2.getType() != (forIsNull ?
				TupleTypes::TYPE_NULL : TupleTypes::TYPE_STRING)) {
			return false;
		}
		else if (column == partitionNameColumn &&
				constValue2.getType() == TupleTypes::TYPE_STRING &&
				TupleString(constValue2).getBuffer().second == 0) {
			return false;
		}

		if (forLike) {
			const TupleValue *constValue3 =
					(expr3 == NULL ? NULL : &expr3->getCode().getValue());
			if (constValue3 != NULL &&
					constValue3->getType() != TupleTypes::TYPE_STRING) {
				return false;
			}

			SQLValues::ValueUtils::unescapeExactLikePattern(
					cxt, constValue2, constValue3, *valueRef);
		}
		else if (forIsNull) {
			*valueRef = SQLValues::ValueUtils::createEmptyValue(
					cxt, TupleTypes::TYPE_STRING);
		}
		else {
			*valueRef =
					SQLValues::ValueUtils::duplicateValue(cxt, constValue2);
		}

		if (forContainer && containerNameElems != NULL) {
			const TupleValue &base = containerNameElems->first;
			const TupleValue &sub = containerNameElems->second;
			if (!SQLValues::ValueUtils::isNull(base) &&
					!SQLValues::ValueUtils::isNull(sub)) {
				if (TupleString(sub).getBuffer().second > 0) {
					SQLValues::StringBuilder builder(cxt);
					SQLValues::ValueUtils::formatValue(cxt, builder, base);
					builder.appendAll("@");
					SQLValues::ValueUtils::formatValue(cxt, builder, sub);
					containerName = builder.build(cxt);
				}
				else {
					containerName = base;
				}
			}
		}

		if (dbName != NULL && SQLValues::ValueUtils::isNull(*dbName)) {
			return false;
		}

		if (SQLValues::ValueUtils::isNull(containerName)) {
			return false;
		}

		return true;
	}
	else {
		return false;
	}
}

bool SQLExprs::ExprRewriter::predicateToContainerId(
		SQLValues::ValueContext &cxt, const ExprFactory &factory,
		const Expression *pred, uint32_t partitionIdColumn,
		uint32_t containerIdColumn, PartitionId &partitionId,
		ContainerId &containerId, bool &placeholderAffected) {
	placeholderAffected = false;

	if (pred == NULL) {
		return false;
	}

	const ExprType predType = pred->getCode().getType();
	if (predType == SQLType::EXPR_AND) {
		const Expression *expr = pred->findChild();
		while (expr != NULL) {
			bool placeholderAffectedLocal;
			const bool resolved = predicateToContainerId(
					cxt, factory, expr, partitionIdColumn, containerIdColumn,
					partitionId, containerId, placeholderAffectedLocal);

			placeholderAffected |= placeholderAffectedLocal;
			if (resolved) {
				return true;
			}

			expr = expr->findNext();
		}
		return false;
	}
	else if (predType == SQLType::OP_EQ) {
		const Expression *expr1 = pred->findChild();
		const Expression *expr2 = (expr1 == NULL ? NULL : expr1->findNext());

		if (expr1->getCode().getType() == SQLType::EXPR_CONSTANT) {
			std::swap(expr1, expr2);
		}

		if (isConstWithPlaceholder(factory, *expr2).first) {
			placeholderAffected = true;
		}

		if (expr2->getCode().getType() != SQLType::EXPR_CONSTANT) {
			return false;
		}

		const Expression &metaExpr = *expr1;
		if (metaExpr.getCode().getType() != SQLType::EXPR_COLUMN) {
			return false;
		}
		const uint32_t column = metaExpr.getCode().getColumnPos();

		const TupleValue &constValue2 = expr2->getCode().getValue();
		if ((SQLValues::TypeUtils::getStaticTypeCategory(
				constValue2.getType()) &
				SQLValues::TypeUtils::TYPE_CATEGORY_NUMERIC_FLAG) == 0) {
			return false;
		}

		int64_t longValue;
		const bool strict = true;
		if (!SQLValues::ValueUtils::toLongDetail(
				constValue2, longValue, strict)) {
			return false;
		}

		if (column == partitionIdColumn) {
			if (longValue < 0 || longValue >= static_cast<int64_t>(
					std::numeric_limits<PartitionId>::max())) {
				return false;
			}
			partitionId = static_cast<PartitionId>(longValue);
		}
		else if (column == containerIdColumn) {
			containerId = static_cast<ContainerId>(longValue);
		}
		else {
			return false;
		}

		return (partitionId != UNDEF_PARTITIONID &&
				containerId != UNDEF_CONTAINERID);
	}
	else {
		return false;
	}
}

bool SQLExprs::ExprRewriter::isColumnMappingRequired() const {
	return entry_->columnMapping_;
}

void SQLExprs::ExprRewriter::remapColumnSub(
		ExprFactoryContext &cxt, Expression &expr) const {

	assert(entry_->columnMapping_);
	ExprCode &code = expr.getPlanningCode();

	bool forId;
	if (code.getType() == SQLType::EXPR_COLUMN) {
		forId = false;
	}
	else if (code.getType() == SQLType::EXPR_ID &&
			(code.getAttributes() & ExprCode::ATTR_GROUPING) == 0 &&
			isIdUsed(code.getInput())) {
		forId = true;
	}
	else {
		for (Expression::ModIterator it(expr); it.exists(); it.next()) {
			remapColumnSub(cxt, it.get());
		}
		return;
	}

	const bool throwOnError = true;
	const uint32_t input = code.getInput();
	const uint32_t column = (forId ? 0 : code.getColumnPos());
	const bool inputUnified =
			((code.getAttributes() & ExprCode::ATTR_COLUMN_UNIFIED) != 0);
	assignRemappedCode(
			cxt, input, column, forId, inputUnified, code, throwOnError);
}

bool SQLExprs::ExprRewriter::assignRemappedCode(
		ExprFactoryContext &cxt, uint32_t input, uint32_t column,
		bool forId, bool inputUnified, ExprCode &code,
		bool throwOnError) const {
	const ExprCode orgCode = code;
	code = ExprCode();

	const ColumnMapElem *elem = findColumnMapElement(input, column, forId);

	const ScopedEntry &entry = getColumnMappingEntry();
	const bool inputNull = entry.inputNullList_[input];
	do {
		if (forId) {
			if (isIdUsed(input) && !entry.idProjected_) {
				if (inputNull) {
					code.setType(SQLType::EXPR_CONSTANT);
				}
				else {
					code.setType(SQLType::EXPR_ID);
					code.setInput(resolveMappedInput(input));
				}
				break;
			}
		}
		else {
			if (inputNull && !entry.inputProjected_) {
				code.setType(SQLType::EXPR_CONSTANT);
				break;
			}
		}

		if (elem == NULL) {
			if (!throwOnError) {
				return false;
			}
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		code.setType(SQLType::EXPR_COLUMN);
		code.setInput(static_cast<uint32_t>(elem->first));
		code.setColumnPos(static_cast<uint32_t>(elem->second));
	}
	while (false);

	{
		TupleColumnType type = orgCode.getColumnType();
		if (SQLValues::TypeUtils::isNull(type)) {
			type = (forId ?
					getIdColumnType(cxt.getFactory()) :
					cxt.getInputType(input, column));
			if (inputNull || cxt.isInputNullable(input)) {
				type = SQLValues::TypeUtils::toNullable(type);
			}
		}
		assert(code.getType() != SQLType::EXPR_CONSTANT ||
				SQLValues::TypeUtils::isNullable(type) ||
				SQLValues::TypeUtils::isAny(type));
		code.setColumnType(type);
	}

	code.setAttributes(orgCode.getAttributes() |
			(inputUnified ? ExprCode::ATTR_COLUMN_UNIFIED : 0));

	return true;
}

const SQLExprs::ExprRewriter::ColumnMapElem&
SQLExprs::ExprRewriter::resolveColumnMapElement(
		uint32_t input, uint32_t column, bool forId) const {
	const ColumnMapElem *elem = findColumnMapElement(input, column, forId);

	if (elem == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *elem;
}

const SQLExprs::ExprRewriter::ColumnMapElem*
SQLExprs::ExprRewriter::findColumnMapElement(
		uint32_t input, uint32_t column, bool forId) const {
	const ColumnMap &map = getColumnMap(forId);
	if (input >= map.size() || column >= map[input].size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const ColumnMapElem &elem = map[input][column];
	if (elem.first < 0 || elem.second < 0) {
		return NULL;
	}

	return &elem;
}

const SQLExprs::ExprRewriter::ColumnMap&
SQLExprs::ExprRewriter::getColumnMap(bool forId) const {
	if (!entry_->columnMapping_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	ColumnMap &columnMap = entry_->columnMap_;
	ColumnMap &idColumnMap = entry_->idColumnMap_;

	if (entry_->columnMapUpdated_) {
		return (forId ? idColumnMap : columnMap);
	}

	const bool inputProjected = entry_->inputProjected_;
	const bool inputOrNullProjected =
			(inputProjected || entry_->inputNullProjected_);

	const bool idProjected = entry_->idProjected_;
	const bool someProjected = (inputOrNullProjected || idProjected);

	const InputMap &inputMap = entry_->inputMap_;

	const UsageList &columnUsage = entry_->columnUsage_;

	const size_t inCount = inputMap.size();
	columnMap.resize(inCount, ColumnMapEntry(alloc_));
	idColumnMap.resize(inCount, ColumnMapEntry(alloc_));

	const ColumnMapElem emptyElem(-1, -1);

	int32_t totalColumn = 0;
	for (UsageList::const_iterator inIt = columnUsage.begin();
			inIt != columnUsage.end(); ++inIt) {
		const int32_t srcInput = static_cast<int32_t>(inIt - columnUsage.begin());
		const Usage &usage = *inIt;

		ColumnMapEntry &destEntry = columnMap[srcInput];
		ColumnMapEntry &destIdEntry = idColumnMap[srcInput];

		destEntry.assign(usage.size(), emptyElem);
		destIdEntry.assign(1, emptyElem);

		int32_t destInput;
		const int32_t mappedInput = inputMap[srcInput];
		if (mappedInput < 0) {
			destInput = srcInput;
		}
		else {
			destInput = mappedInput;
		}

		if (isIdUsed(srcInput)) {
			ColumnMapElem &dest = destIdEntry.front();
			dest = ColumnMapElem(
					(inputProjected ? 0 : destInput),
					(idProjected ? totalColumn : -1));
			if (idProjected) {
				totalColumn++;
			}
		}

		for (Usage::const_iterator it = usage.begin();
				it != usage.end(); ++it) {
			if (!(*it)) {
				continue;
			}

			const int32_t ordinal = static_cast<int32_t>(it - usage.begin());
			ColumnMapElem &dest = destEntry[static_cast<size_t>(ordinal)];
			dest = ColumnMapElem(
					(inputProjected ? 0 : destInput),
					(someProjected ? totalColumn : ordinal));
			if (someProjected) {
				totalColumn++;
			}
		}
		if (!inputOrNullProjected) {
			totalColumn = 0;
		}
	}

	entry_->columnMapUpdated_ = true;
	return getColumnMap(forId);
}

uint32_t SQLExprs::ExprRewriter::resolveMappedInput(uint32_t src) const {
	const InputMap &map = getColumnMappingEntry().inputMap_;
	if (src >= map.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const int32_t dest = map[src];
	if (dest < 0) {
		return src;
	}

	return static_cast<uint32_t>(dest);
}

bool SQLExprs::ExprRewriter::isIdUsed(uint32_t input) const {
	if (!entry_->columnMapping_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const Usage &inputUsage = entry_->inputUsage_;
	if (!inputUsage[input]) {
		return false;
	}

	const Usage &idUsage = entry_->idUsage_;
	if (idUsage[input]) {
		return true;
	}

	const Usage &usage = entry_->columnUsage_[input];
	const bool emptyUsage =
			(std::find(usage.begin(), usage.end(), true) == usage.end());
	if (emptyUsage) {
		return true;
	}

	return false;
}

SQLExprs::ExprRewriter::ScopedEntry&
SQLExprs::ExprRewriter::getColumnMappingEntry() {
	if (!entry_->columnMapping_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	entry_->columnMapUpdated_ = false;
	return *entry_;
}

const SQLExprs::ExprRewriter::ScopedEntry&
SQLExprs::ExprRewriter::getColumnMappingEntry() const {
	if (!entry_->columnMapping_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return *entry_;
}

bool SQLExprs::ExprRewriter::isCodeSetUpRequired(
		const Expression &src) const {
	if (entry_->codeSetUpAlways_) {
		return true;
	}

	const ExprCode &code = src.getCode();
	if (SQLValues::TypeUtils::isNull(code.getColumnType()) &&
			!ExprTypeUtils::isNoColumnTyped(code.getType())) {
		return true;
	}
	if ((code.getAttributes() & ExprCode::ATTR_ASSIGNED) == 0) {
		return true;
	}
	return false;
}

uint32_t SQLExprs::ExprRewriter::prepareTopAttributes(
		const Expression &expr, bool forProj, bool top) const {
	uint32_t attributes = (top ? expr.getCode().getAttributes() : 0);

	if (forProj) {
		if (ExprTypeUtils::isAggregation(expr.getCode().getType())) {
			attributes |= ExprCode::ATTR_AGGREGATED;
		}
		else {
			for (Expression::Iterator it(expr); it.exists(); it.next()) {
				attributes |= prepareTopAttributes(it.get(), forProj, false);
				if (attributes != 0) {
					break;
				}
			}
		}
	}

	return attributes;
}

void SQLExprs::ExprRewriter::setUpCodes(
		ExprFactoryContext &cxt, Expression &expr, const size_t *projDepth,
		uint32_t topAttributes, bool insideAggrFunc) const {
	const ExprType exprType = expr.getCode().getType();

	const bool insideAggrFuncSub = insideAggrFunc ||
			(exprType != SQLType::AGG_FIRST &&
			ExprTypeUtils::isAggregation(exprType));

	for (Expression::ModIterator it(expr); it.exists(); it.next()) {
		size_t subDepth = (projDepth == NULL ? 0 : *projDepth) + 1;
		setUpCodes(
				cxt, it.get(), (projDepth == NULL ? NULL : &subDepth),
				topAttributes, insideAggrFuncSub);
	}

	setUpCodesAt(cxt, expr, projDepth, topAttributes, insideAggrFunc);
	optimizeExpr(cxt, expr);
}

void SQLExprs::ExprRewriter::setUpCodesAt(
		ExprFactoryContext &cxt, Expression &expr, const size_t *projDepth,
		uint32_t topAttributes, bool insideAggrFunc) const {

	ExprCode &code = expr.getPlanningCode();
	code.setAttributes(
			resolveAttributesAt(expr, projDepth, topAttributes, insideAggrFunc));
	code.setColumnType(resolveColumnTypeAt(
			cxt, expr, projDepth, topAttributes, insideAggrFunc));

	assert(entry_->codeSetUpAlways_ || !isCodeSetUpRequired(expr));
}

uint32_t SQLExprs::ExprRewriter::resolveAttributesAt(
		const Expression &expr, const size_t *projDepth,
		uint32_t topAttributes, bool insideAggrFunc) {
	const ExprCode &code = expr.getCode();

	bool aggregated = false;
	if (ExprTypeUtils::isAggregation(expr.getCode().getType())) {
		aggregated = true;
	}
	else if (projDepth != NULL && *projDepth == 0 &&
			(topAttributes & ExprCode::ATTR_GROUPING) != 0) {
		aggregated = true;
	}
	else {
		for (Expression::Iterator it(expr); it.exists(); it.next()) {
			if ((it.get().getCode().getAttributes() &
					ExprCode::ATTR_AGGREGATED) != 0) {
				aggregated = true;
				break;
			}
		}
	}

	if (!aggregated && !insideAggrFunc &&
			(expr.getCode().getType() == SQLType::EXPR_RANGE_KEY_CURRENT ||
			expr.getCode().getType() == SQLType::EXPR_RANGE_KEY ||
			expr.getCode().getType() == SQLType::EXPR_RANGE_FILL)) {
		aggregated = true;
	}

	uint32_t attributes = (code.getAttributes() | ExprCode::ATTR_ASSIGNED);
	if (aggregated) {
		attributes |= ExprCode::ATTR_AGGREGATED;
	}

	return attributes;
}

TupleColumnType SQLExprs::ExprRewriter::resolveColumnTypeAt(
		ExprFactoryContext &cxt, const Expression &expr,
		const size_t *projDepth, uint32_t topAttributes,
		bool insideAggrFunc) const {
	static_cast<void>(projDepth);

	const ExprCode &code = expr.getCode();

	const TupleColumnType baseType = code.getColumnType();

	TupleColumnType type;
	bool nullable = false;
	const ExprType exprType = code.getType();
	if (exprType == SQLType::EXPR_CONSTANT) {
		type = SQLValues::ValueUtils::toColumnType(code.getValue());
	}
	else if (exprType == SQLType::OP_CAST) {
		const Expression &arg2 = expr.child().next();
		if (arg2.getCode().getType() != SQLType::EXPR_TYPE) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION,
					"The second of cast args must be type expression");
		}

		type = arg2.getCode().getColumnType();

		if (!SQLValues::TypeUtils::isDeclarable(
				SQLValues::TypeUtils::toNonNullable(type))) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION,
					"Unsupported cast type (type=" <<
					SQLValues::TypeUtils::toString(type) << ")");
		}
	}
	else if (exprType == SQLType::EXPR_COLUMN) {
		if ((code.getAttributes() & ExprCode::ATTR_COLUMN_UNIFIED) != 0) {
			type = cxt.getUnifiedInputType(code.getColumnPos());
		}
		else {
			type = cxt.getInputType(code.getInput(), code.getColumnPos());
			nullable = cxt.isInputNullable(code.getInput());
		}
		if ((topAttributes & ExprCode::ATTR_AGGREGATED) != 0 &&
				(topAttributes & ExprCode::ATTR_GROUPING) == 0 &&
				!insideAggrFunc) {
			nullable = true;
		}
	}
	else if (ExprTypeUtils::isNoColumnTyped(exprType) ||
			exprType == SQLType::EXPR_AGG_FOLLOWING) {
		return baseType;
	}
	else if (entry_->codeSetUpAlways_ &&
			SQLExprs::ExprTypeUtils::isAggregation(exprType) &&
			cxt.getAggregationPhase(true) == SQLType::END_AGG_PHASE) {
		type = baseType;
	}
	else {
		const ExprSpec &spec = cxt.getFactory().getSpec(exprType);
		const SQLType::AggregationPhase aggrPhase = (entry_->codeSetUpAlways_ ?
				SQLType::AGG_PHASE_ALL_PIPE : cxt.getAggregationPhase(true));
		const bool grouping = ((topAttributes & ExprCode::ATTR_GROUPING) != 0);

		const TypeResolverResult &retInfo = resolveBasicExprColumnTypes(
				expr, spec, aggrPhase, grouping, false);
		type = retInfo.typeList_[0];
	}

	if (nullable) {
		type = SQLValues::TypeUtils::toNullable(type);
	}

	if (SQLValues::TypeUtils::isNull(type)) {
		const bool throwOnError = true;
		checkArgCount(
				cxt.getFactory(), exprType, expr.getChildCount(),
				cxt.getAggregationPhase(true), throwOnError);
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION,"");
	}

	if (SQLValues::TypeUtils::isNull(baseType)) {
		return type;
	}

	if (SQLValues::TypeUtils::toNonNullable(type) ==
			SQLValues::TypeUtils::toNonNullable(baseType)) {
		if (type != baseType &&
				!SQLExprs::ExprTypeUtils::isAggregation(exprType) &&
				((SQLValues::TypeUtils::isNullable(baseType) &&
						(projDepth == NULL || *projDepth > 1)) ||
				entry_->codeSetUpAlways_)) {
			return type;
		}
		return baseType;
	}
	else if (SQLValues::TypeUtils::isAny(type) && !SQLValues::TypeUtils::isNull(
			SQLValues::TypeUtils::findPromotionType(type, baseType, true))) {
		return baseType;
	}
	else if (exprType == SQLType::EXPR_COLUMN &&
			SQLValues::TypeUtils::toNonNullable(baseType) ==
			SQLValues::TypeUtils::toNonNullable(
					SQLValues::TypeUtils::findPromotionType(
							type, baseType, true))) {
		return baseType;
	}

	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION,
			"Expression column type unmatched (specified=" <<
			SQLValues::TypeUtils::toString(type) <<
			", expected=" <<
			SQLValues::TypeUtils::toString(baseType) << ")");
}

SQLExprs::TypeResolverResult
SQLExprs::ExprRewriter::resolveBasicExprColumnTypes(
		const Expression &expr, const ExprSpec &spec,
		AggregationPhase aggrPhase, bool grouping, bool checking) {

	SQLExprs::TypeResolver resolver(spec, aggrPhase, grouping);
	for (Expression::Iterator it(expr); it.exists(); it.next()) {
		resolver.next(it.get().getCode().getColumnType());
	}
	return resolver.complete(checking);
}

SQLExprs::TypeResolverResult
SQLExprs::ExprRewriter::resolveNonDistinctExprColumnTypes(
		const ExprFactory &factory, const Expression &src,
		ExprType *destExprType) {

	const ExprType srcExprType = src.getCode().getType();
	const ExprSpec &srcSpec = factory.getSpec(srcExprType);
	assert(isDistinctAggregation(srcSpec, srcExprType));

	const ExprType destExprTypeBase = srcSpec.distinctExprType_;
	const ExprSpec &destSpec = factory.getSpec(destExprTypeBase);

	if (destExprType != NULL) {
		*destExprType = destExprTypeBase;
	}
	const bool grouping = true;
	return resolveBasicExprColumnTypes(
			src, destSpec, SQLType::AGG_PHASE_ALL_PIPE, grouping, true);
}

void SQLExprs::ExprRewriter::optimizeExpr(
		ExprFactoryContext &cxt, Expression &expr) {
	const ExprType type = expr.getCode().getType();
	if (ExprTypeUtils::isCompOp(type, true)) {
		optimizeCompExpr(cxt, expr);
	}
	else if (type == SQLType::EXPR_OR) {
		optimizeOrExpr(cxt, expr);
	}
}

void SQLExprs::ExprRewriter::optimizeCompExpr(
		ExprFactoryContext &cxt, Expression &expr) {
	const Expression *left = &expr.child();
	const Expression *right = &left->next();

	bool swapRequired = false;
	do {
		const ExprType exprType1 = left->getCode().getType();
		const ExprType exprType2 = right->getCode().getType();

		if (exprType1 == SQLType::EXPR_COLUMN &&
				exprType2 == SQLType::EXPR_CONSTANT) {
			break;
		}

		if (exprType1 == SQLType::EXPR_CONSTANT &&
				exprType2 == SQLType::EXPR_COLUMN) {
			swapRequired = true;
			break;
		}

		const TupleColumnType type1 = SQLValues::TypeUtils::toNonNullable(
				left->getCode().getColumnType());
		const TupleColumnType type2 = SQLValues::TypeUtils::toNonNullable(
				right->getCode().getColumnType());

		if (SQLValues::TypeUtils::isTimestampFamily(type1)) {
			swapRequired = (
					SQLValues::TypeUtils::getTimePrecisionLevel(type1) <
					SQLValues::TypeUtils::getTimePrecisionLevel(type2));
			break;
		}

		if (!SQLValues::TypeUtils::isNumerical(type1) ||
				!SQLValues::TypeUtils::isNumerical(type2)) {
			break;
		}

		const bool floating1 = SQLValues::TypeUtils::isFloating(type1);
		const bool floating2 = SQLValues::TypeUtils::isFloating(type2);
		if (floating1 != floating2) {
			if (!floating1 && floating2) {
				swapRequired = true;
			}
			break;
		}

		swapRequired = (
				SQLValues::TypeUtils::getFixedSize(type1) <
				SQLValues::TypeUtils::getFixedSize(type2));
	}
	while (false);

	if (swapRequired) {
		swapExprArgs(expr);
		left = &expr.child();
		right = &left->next();
		expr.getPlanningCode().setType(
				ExprTypeUtils::swapCompOp(expr.getCode().getType()));
	}

	TupleColumnType desiredType = TupleTypes::TYPE_NULL;
	TupleColumnType adjustingType = TupleTypes::TYPE_NULL;
	bool adjustOnLossy = false;
	do {
		const ExprType exprType1 = left->getCode().getType();
		const ExprType exprType2 = right->getCode().getType();

		if (exprType1 != SQLType::EXPR_COLUMN ||
				exprType2 != SQLType::EXPR_CONSTANT) {
			break;
		}

		const TupleValue &srcValue = right->getCode().getValue();
		if (SQLValues::ValueUtils::isNull(srcValue)) {
			break;
		}

		const TupleColumnType type1 = SQLValues::TypeUtils::toNonNullable(
				left->getCode().getColumnType());
		const TupleColumnType type2 = SQLValues::TypeUtils::toNonNullable(
				srcValue.getType());

		if (SQLValues::TypeUtils::isTimestampFamily(type1)) {
			if (type1 == type2) {
				break;
			}
			desiredType = type1;
			adjustingType = TupleTypes::TYPE_NANO_TIMESTAMP;
			adjustOnLossy = true;
			break;
		}

		if (SQLValues::TypeUtils::isNumerical(type1) &&
				SQLValues::TypeUtils::isNumerical(type2)) {
			const bool floating1 = SQLValues::TypeUtils::isFloating(type1);
			const bool floating2 = SQLValues::TypeUtils::isFloating(type2);

			TupleColumnType baseType;
			if (!floating1 && floating2) {
				baseType = TupleTypes::TYPE_DOUBLE;
			}
			else {
				baseType = SQLValues::TypeUtils::toComparisonType(type1);
			}

			if (baseType != type2) {
				adjustingType = baseType;
			}

			break;
		}
	}
	while (false);

	if (!SQLValues::TypeUtils::isNull(adjustingType)) {
		util::StackAllocator &alloc = cxt.getAllocator();
		SQLValues::ValueContext valueCxt(
				SQLValues::ValueContext::ofAllocator(alloc));

		const TupleValue &srcValue = right->getCode().getValue();

		TupleValue destValue;
		TupleColumnType valueType;
		if (adjustOnLossy) {
			destValue = SQLValues::ValueConverter(desiredType)(
					valueCxt, srcValue);

			const bool sensitive = false;
			const bool lossy = (SQLValues::ValueComparator::compareValues(
					destValue, srcValue, sensitive, true) != 0);
			if (lossy) {
				destValue = SQLValues::ValueConverter(
						adjustingType)(valueCxt, srcValue);
			}
			valueType = (lossy ? adjustingType : desiredType);
		}
		else {
			destValue = SQLValues::ValueConverter(
					adjustingType)(valueCxt, srcValue);
			valueType = adjustingType;
		}

		Expression::ModIterator it(expr);
		it.next();

		ExprCode &destCode = it.get().getPlanningCode();
		destCode.setValue(valueCxt, destValue);
		destCode.setColumnType(valueType);
	}
}

void SQLExprs::ExprRewriter::optimizeOrExpr(
		ExprFactoryContext &cxt, Expression &expr) {
	assert(expr.getCode().getType() == SQLType::EXPR_OR);

	ExprProfile *profile = cxt.getProfile();
	SQLValues::ProfileElement emptyProfile;
	SQLValues::ProfileElement &condProfile =
			(profile == NULL ? emptyProfile : profile->condInList_);
	condProfile.candidate_++;

	bool inExprFound = false;
	const Expression *lastKeyExpr = NULL;
	TupleColumnType lastValueType = TupleTypes::TYPE_NULL;
	for (Expression::Iterator it(expr); it.exists(); it.next()) {
		const Expression &subExpr = it.get();
		const ExprType subType = subExpr.getCode().getType();

		const Expression *valueExpr;
		if (subType == SQLType::OP_EQ) {
			const Expression &subRight = subExpr.child().next();
			if (subRight.getCode().getType() != SQLType::EXPR_CONSTANT) {
				return;
			}
			valueExpr = &subRight;
		}
		else if (subType == SQLType::EXPR_IN) {
			const Expression &subRight = subExpr.child().next();
			valueExpr = &subRight.child();
			if (valueExpr->findNext() != NULL &&
					SQLValues::TypeUtils::isNull(
							valueExpr->getCode().getValue().getType())) {
				valueExpr = &valueExpr->next();
			}
			inExprFound = true;
		}
		else {
			return;
		}

		const TupleColumnType valueType =
				valueExpr->getCode().getValue().getType();
		if (!SQLValues::TypeUtils::isNull(lastValueType) &&
				!SQLValues::TypeUtils::isNull(valueType) &&
				lastValueType != valueType) {
			return;
		}

		const Expression &keyExpr = subExpr.child();
		if (lastKeyExpr != NULL &&
				!isSameExpr(cxt.getFactory(), *lastKeyExpr, keyExpr, true)) {
			return;
		}

		lastKeyExpr = &keyExpr;
		if (!SQLValues::TypeUtils::isNull(valueType)) {
			lastValueType = valueType;
		}
	}
	condProfile.target_++;

	if (inExprFound && expr.child().getCode().getType() != SQLType::EXPR_IN) {
		swapExprArgs(expr);
	}

	Expression *destKeyExpr = NULL;
	Expression *destNullExpr = NULL;
	Expression *destListExpr = NULL;
	util::LocalUniquePtr<Expression::ModIterator> listIt;
	for (Expression::ModIterator it(expr); it.exists();) {
		Expression &subExpr = it.get();
		it.remove();

		Expression *srcCondExpr = NULL;
		{
			Expression::ModIterator subIt(subExpr);
			if (destKeyExpr == NULL) {
				destKeyExpr = &subIt.get();
			}
			subIt.remove();

			srcCondExpr = &subIt.get();
			subIt.remove();
		}

		if (listIt.get() == NULL) {
			if (srcCondExpr->getCode().getType() == SQLType::EXPR_LIST) {
				destListExpr = srcCondExpr;
				srcCondExpr = NULL;
			}
			else {
				destListExpr = &cxt.getFactory().create(cxt, SQLType::EXPR_LIST);
				destListExpr->getPlanningCode().setAttributes(
						ExprCode::ATTR_ASSIGNED);
			}
			listIt = UTIL_MAKE_LOCAL_UNIQUE(
					listIt, Expression::ModIterator, *destListExpr);

			if (listIt->exists() && SQLValues::TypeUtils::isNull(
					listIt->get().getCode().getValue().getType())) {
				destNullExpr = &listIt->get();
				listIt->remove();
			}
			while (listIt->exists()) {
				listIt->next();
			}
			if (srcCondExpr == NULL) {
				continue;
			}
		}

		if (srcCondExpr->getCode().getType() == SQLType::EXPR_LIST) {
			for (Expression::ModIterator srcIt(*srcCondExpr); srcIt.exists();) {
				Expression &valueExpr = srcIt.get();
				srcIt.remove();

				if (SQLValues::TypeUtils::isNull(
						valueExpr.getCode().getValue().getType())) {
					if (destNullExpr == NULL) {
						destNullExpr = &valueExpr;
					}
					continue;
				}
				listIt->append(valueExpr);
			}
		}
		else {
			listIt->append(*srcCondExpr);
		}
	}

	{
		TupleColumnType listType = lastValueType;
		if (SQLValues::TypeUtils::isNull(listType)) {
			listType = TupleTypes::TYPE_ANY;
		}
		if (destNullExpr != NULL) {
			listType = SQLValues::TypeUtils::toNullable(listType);
		}
		destListExpr->getPlanningCode().setColumnType(listType);

		if ((destKeyExpr->getCode().getAttributes() &
				ExprCode::ATTR_AGGREGATED) != 0) {
			destListExpr->getPlanningCode().setAttributes(
					destListExpr->getCode().getAttributes() |
					ExprCode::ATTR_AGGREGATED);
		}
	}

	if (destNullExpr != NULL) {
		listIt = UTIL_MAKE_LOCAL_UNIQUE(
				listIt, Expression::ModIterator, *destListExpr);
		listIt->insert(*destNullExpr);
		listIt.reset();
	}

	{
		Expression::ModIterator it(expr);
		assert(!it.exists());
		it.append(*destKeyExpr);
		it.append(*destListExpr);
	}

	expr.getPlanningCode().setType(SQLType::EXPR_IN);
}

void SQLExprs::ExprRewriter::swapExprArgs(Expression &expr) {
	Expression::ModIterator it(expr);

	assert(it.exists());
	Expression &left = it.get();
	it.remove();

	assert(it.exists());
	Expression &right = it.get();
	it.remove();

	it.append(right);
	it.append(left);
}

bool SQLExprs::ExprRewriter::isSameExpr(
		const ExprFactory &factory, const Expression &expr1,
		const Expression &expr2, bool excludesDynamic) {
	if (!isSameExprCode(expr1.getCode(), expr2.getCode())) {
		return false;
	}

	do {
		if (!excludesDynamic) {
			break;
		}

		const ExprType type = expr1.getCode().getType();
		if (type == SQLType::EXPR_COLUMN) {
			break;
		}

		const ExprSpec &spec = factory.getSpec(type);
		if ((spec.flags_ & ExprSpec::FLAG_DYNAMIC) != 0) {
			return false;
		}
	}
	while (false);

	Expression::Iterator it1(expr1);
	Expression::Iterator it2(expr2);
	while (it1.exists() && it2.exists()) {
		if (!isSameExpr(factory, it1.get(), it2.get(), excludesDynamic)) {
			return false;
		}
		it1.next();
		it2.next();
	}

	return (!it1.exists() && !it2.exists());
}

bool SQLExprs::ExprRewriter::isSameExprCode(
		const ExprCode &code1, const ExprCode &code2) {
	return (code1.getType() == code2.getType() &&
			code1.getColumnType() == code2.getColumnType() &&
			code1.getInput() == code2.getInput() &&
			code1.getColumnPos() == code2.getColumnPos() &&
			code1.getValue().getType() == code2.getValue().getType() &&
			SQLValues::ValueEq()(code1.getValue(), code2.getValue()) &&
			code1.getOutput() == code2.getOutput() &&
			code1.getAggregationIndex() == code2.getAggregationIndex() &&
			code1.getAttributes() == code2.getAttributes());
}

template<typename T>
bool SQLExprs::ExprRewriter::checkNumericBounds(const TupleValue &value) {
	typedef typename T::ValueType ValueType;
	{
		const TupleValue &min = SQLValues::ValueUtils::toAnyByNumeric(
				std::numeric_limits<ValueType>::min());
		if (SQLValues::ValueLess()(value, min)) {
			return false;
		}
	}
	{
		const TupleValue &max = SQLValues::ValueUtils::toAnyByNumeric(
				std::numeric_limits<ValueType>::max());
		if (SQLValues::ValueLess()(max, value)) {
			return false;
		}
	}
	return true;
}

bool SQLExprs::ExprRewriter::isAggregationSetUpRequired(
		ExprFactoryContext &cxt, const Expression &src,
		bool forProjection) const {
	if (!forProjection ||
			cxt.getAggregationPhase(false) == SQLType::END_AGG_PHASE) {
		return false;
	}

	const ExprCode &code = src.getCode();
	if ((code.getAttributes() &
			(ExprCode::ATTR_AGGREGATED |
			ExprCode::ATTR_GROUPING)) == 0) {
		return false;
	}
	if ((code.getAttributes() & ExprCode::ATTR_AGGR_ARRANGED) != 0) {
		return false;
	}
	return true;
}

void SQLExprs::ExprRewriter::setUpAggregation(
		ExprFactoryContext &cxt, Expression &expr) const {
	const ExprCode &code = expr.getCode();
	static_cast<void>(code);

	assert((code.getAttributes() & ExprCode::ATTR_ASSIGNED) != 0);
	assert((code.getAttributes() &
			(ExprCode::ATTR_AGGREGATED | ExprCode::ATTR_GROUPING)) != 0);
	assert(!(entry_->multiStageGrouping_ &&
			findDistinctAggregation(cxt.getFactory(), expr)));

	prepareMultiStageGrouping(cxt);
	prepareDistinctAggregation();

	{
		cxt.clearAggregationColumns();
		if (cxt.getAggregationPhase(true) ==
				SQLType::AGG_PHASE_ADVANCE_PIPE) {
			setUpAdvanceAggregation(cxt, expr, expr.getCode());
		}
		else {
			setUpMergeAggregation(cxt, expr, expr.getCode());
		}
		setAggregationArranged(expr);
	}

	if (isFinishAggregationArranging(cxt)) {
		if (entry_->outputMiddle_) {
			setUpMiddleFinishAggregation(cxt, expr);
		}
		else {
			setUpFinishAggregation(cxt, expr, expr.getCode());
			setUpDistinctFinishAggregation(cxt, expr);
			remapMultiStageGroupColumns(cxt, expr, true);
		}
	}
	else {
		setUpPipeAggregation(cxt, expr, NULL);
		setUpDistinctPipeAggregation(cxt, expr);
		remapMultiStageGroupColumns(cxt, expr, false);
	}

	assert(!isAggregationSetUpRequired(cxt, expr, true));
}

bool SQLExprs::ExprRewriter::isFinishAggregationArranging(
		ExprFactoryContext &cxt) {
	const AggregationPhase destPhase = cxt.getAggregationPhase(false);
	return (destPhase == SQLType::AGG_PHASE_MERGE_FINISH ||
			destPhase == SQLType::AGG_PHASE_ADVANCE_FINISH);
}

void SQLExprs::ExprRewriter::setUpAdvanceAggregation(
		ExprFactoryContext &cxt, Expression &expr,
		const ExprCode &topCode) const {
	for (Expression::ModIterator it(expr); it.exists();) {
		const ExprType type = it.get().getCode().getType();
		const TupleColumnType columnType = it.get().getCode().getColumnType();

		if (type == SQLType::EXPR_AGG_FOLLOWING) {
			cxt.addAggregationColumn(columnType);
			it.remove();
			continue;
		}
		else if (ExprTypeUtils::isAggregation(type)) {
			if (isDistinctAggregation(cxt.getFactory(), type)) {
				it.next();
				while (it.exists() && (it.get().getCode().getType() ==
						SQLType::EXPR_AGG_FOLLOWING)) {
					it.remove();
				}
				entry_->distinctAggrFound_ = true;
				continue;
			}

			const uint32_t aggrIndex = cxt.addAggregationColumn(columnType);
			it.get().getPlanningCode().setAggregationIndex(aggrIndex);
		}
		else if (type == SQLType::EXPR_CONSTANT ||
				matchMultiStageGroupKey(it.get(), NULL)) {
			it.next();
			continue;
		}
		else {
			replaceChildToAggrFirst(cxt, it);
			setUpAggregationColumnsAt(cxt, it.get(), topCode);
		}

		assert((it.get().getCode().getAttributes() &
				ExprCode::ATTR_AGGREGATED) != 0);

		it.next();
	}
}

void SQLExprs::ExprRewriter::setUpMergeAggregation(
		ExprFactoryContext &cxt, Expression &expr,
		const ExprCode &topCode) const {
	const ExprType exprType = expr.getCode().getType();
	if (ExprTypeUtils::isAggregation(exprType)) {
		setUpAggregationColumnsAt(cxt, expr, topCode);
		setAggregationArranged(expr);
		return;
	}

	assert((expr.getCode().getAttributes() & ExprCode::ATTR_AGGREGATED) != 0 ||
			(topCode.getAttributes() & ExprCode::ATTR_WINDOWING) != 0);

	for (Expression::ModIterator it(expr); it.exists(); it.next()) {
		const ExprCode &code = it.get().getCode();
		if (!ExprTypeUtils::isAggregation(code.getType())) {
			if ((code.getAttributes() & ExprCode::ATTR_AGGREGATED) != 0) {
				setUpMergeAggregation(cxt, it.get(), topCode);
				continue;
			}
			else if (code.getType() == SQLType::EXPR_TYPE ||
					code.getType() == SQLType::EXPR_CONSTANT ||
					matchMultiStageGroupKey(it.get(), NULL)) {
				continue;
			}

			if ((topCode.getAttributes() & ExprCode::ATTR_WINDOWING) == 0) {
				replaceChildToAggrFirst(cxt, it);
			}
		}
		setUpMergeAggregation(cxt, it.get(), topCode);
	}
}

void SQLExprs::ExprRewriter::setUpAggregationColumnsAt(
		ExprFactoryContext &cxt, Expression &expr,
		const ExprCode &topCode) const {
	const ExprType exprType = expr.getCode().getType();
	const ExprSpec &spec = cxt.getFactory().getSpec(exprType);
	if (isDistinctAggregation(spec, exprType)) {
		entry_->distinctAggrFound_ = true;
		return;
	}

	if (cxt.getAggregationPhase(true) == SQLType::AGG_PHASE_MERGE_PIPE) {
		bool first = true;
		for (Expression::Iterator it(expr); it.exists(); it.next()) {
			TupleColumnType type = it.get().getCode().getColumnType();
			if (exprType == SQLType::AGG_FIRST) {
				type = SQLValues::TypeUtils::toNullable(type);
			}
			const uint32_t aggrIndex = cxt.addAggregationColumn(type);
			if (first) {
				expr.getPlanningCode().setAggregationIndex(aggrIndex);
				first = false;
			}
		}
	}
	else {
		const bool grouping =
				((topCode.getAttributes() & ExprCode::ATTR_GROUPING) != 0);
		const TypeResolverResult &retInfo = resolveBasicExprColumnTypes(
				expr, spec, SQLType::AGG_PHASE_ADVANCE_PIPE, grouping, true);

		for (size_t i = 0; i < TypeResolver::RET_TYPE_LIST_SIZE; i++) {
			const TupleColumnType type = retInfo.typeList_[i];
			if (SQLValues::TypeUtils::isNull(type)) {
				assert(i > 0);
				break;
			}
			const uint32_t aggrIndex = cxt.addAggregationColumn(type);
			if (i == 0) {
				expr.getPlanningCode().setAggregationIndex(aggrIndex);
			}
		}
	}
}

void SQLExprs::ExprRewriter::setUpPipeAggregation(
		ExprFactoryContext &cxt, Expression &expr,
		Expression::ModIterator *destIt) const {
	if (destIt == NULL) {
		Expression &srcExpr = replaceParentToPlanning(cxt, expr);
		Expression::ModIterator localDestIt(expr);
		setUpPipeAggregation(cxt, srcExpr, &localDestIt);
		return;
	}

	for (Expression::ModIterator it(expr); it.exists();) {
		Expression &sub = it.get();

		const ExprType subType = sub.getCode().getType();
		if (ExprTypeUtils::isAggregation(subType)) {
			it.remove();

			if (isDistinctAggregation(cxt.getFactory(), subType)) {
				entry_->distinctAggrSubList_.push_back(&sub);
			}
			else {
				destIt->append(sub);
			}
			continue;
		}

		setUpPipeAggregation(cxt, sub, destIt);
		it.next();
	}
}

void SQLExprs::ExprRewriter::setUpFinishAggregation(
		ExprFactoryContext &cxt, Expression &expr,
		const ExprCode &topCode) const {

	const ExprType exprType = expr.getCode().getType();
	const bool childrenUnused = ExprTypeUtils::isAggregation(exprType) &&
			(exprType != SQLType::AGG_FIRST ||
			(topCode.getAttributes() & ExprCode::ATTR_GROUPING) != 0);
	const bool forAdvance =
			(cxt.getAggregationPhase(false) ==
			SQLType::AGG_PHASE_ADVANCE_FINISH);

	for (Expression::ModIterator it(expr); it.exists();) {
		const ExprCode &subCode = it.get().getCode();
		const ExprType subType = subCode.getType();

		if (childrenUnused) {
			it.remove();
			continue;
		}

		if (matchMultiStageGroupKey(it.get(), NULL)) {
			it.next();
			continue;
		}

		if (forAdvance && subType != SQLType::AGG_FIRST &&
				ExprTypeUtils::isAggregation(subType)) {
			replaceChildToAdvanceOutput(cxt, it);
		}
		else if (isDistinctAggregation(cxt.getFactory(), subType)) {
			replaceChildToDistinctFinishOutput(cxt, it);
		}
		else if (subType == SQLType::EXPR_COLUMN) {
			if ((topCode.getAttributes() & ExprCode::ATTR_WINDOWING) == 0) {
				assert((subCode.getAttributes() &
						ExprCode::ATTR_AGGREGATED) == 0);
				replaceChildToNull(cxt, it);
			}
		}
		else if (subType == SQLType::EXPR_ID) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION,"");
		}
		else {
			setUpFinishAggregation(cxt, it.get(), topCode);

			if (!forAdvance && subType != SQLType::AGG_FIRST &&
					ExprTypeUtils::isAggregation(subType)) {
				if ((cxt.getFactory().getSpec(subType).flags_ &
						ExprSpec::FLAG_AGGR_FINISH_DEFAULT) != 0) {
					it.get().getPlanningCode().setType(SQLType::AGG_FIRST);
				}
			}

			if (entry_->distinctAggrFound_ &&
					ExprTypeUtils::isAggregation(subType)) {
				replaceChildToDistinctFinishOutput(cxt, it);
			}
		}
		it.next();
	}
}

void SQLExprs::ExprRewriter::setUpDistinctPipeAggregation(
		ExprFactoryContext &cxt, const Expression &expr) const {
	if (!entry_->distinctAggrFound_) {
		return;
	}

	assert(!entry_->distinctAggrSubList_.empty());

	Expression &outExpr = cxt.getFactory().create(cxt, SQLType::EXPR_PROJECTION);
	Expression::ModIterator outIt(outExpr);

	outIt.append(createDistinctAggregationIdExpr(cxt));

	for (ExprRefList::const_iterator it = entry_->distinctAggrSubList_.begin();
			it != entry_->distinctAggrSubList_.end(); ++it) {
		for (Expression::ModIterator subIt(**it); subIt.exists();) {
			Expression &subArgExpr = subIt.get();
			subIt.remove();
			outIt.append(subArgExpr);
		}
	}

	outExpr.getPlanningCode().setAttributes(expr.getCode().getAttributes());

	entry_->distinctAggrSubList_.clear();
	entry_->distinctAggrsExpr_ = &outExpr;
}

void SQLExprs::ExprRewriter::setUpDistinctFinishAggregation(
		ExprFactoryContext &cxt, Expression &expr) const {
	if (!entry_->distinctAggrFound_) {
		return;
	}

	assert(expr.findChild() != NULL);

	Expression::ModIterator srcIt(expr);

	Expression &outExpr = cxt.getFactory().create(cxt, SQLType::EXPR_PROJECTION);
	Expression::ModIterator outIt(outExpr);

	while (srcIt.exists()) {
		Expression &subExpr = srcIt.get();
		srcIt.remove();

		outIt.append(subExpr);
	}

	srcIt.append(createDistinctAggregationIdExpr(cxt));

	for (ExprRefList::const_iterator it = entry_->distinctAggrMainList_.begin();
			it != entry_->distinctAggrMainList_.end(); ++it) {
		srcIt.append(**it);
	}

	outExpr.getPlanningCode().setAttributes(expr.getCode().getAttributes());

	entry_->distinctAggrMainList_.clear();
	entry_->distinctAggrsExpr_ = &outExpr;
}

SQLExprs::Expression& SQLExprs::ExprRewriter::createDistinctAggregationIdExpr(
		ExprFactoryContext &cxt) {
	const uint32_t input = 0;
	Expression &expr = createTypedIdExpr(cxt, input);

	ExprCode &code = expr.getPlanningCode();
	code.setAttributes(code.getAttributes() | ExprCode::ATTR_GROUPING);

	return expr;
}

void SQLExprs::ExprRewriter::prepareMultiStageGrouping(
		ExprFactoryContext &cxt) const {
	if (!entry_->multiStageGrouping_) {
		return;
	}


	KeyMap &keyMap = entry_->multiStageKeyMap_;
	keyMap.clear();

	bool &withDigest = entry_->multiStageKeyWithDigest_;
	withDigest = false;

	uint32_t &keyCount = entry_->multiStageKeyCount_;
	keyCount = 0;

	bool orderingRestricted;
	const SQLValues::CompColumnList *keyList =
			cxt.getArrangedKeyList(orderingRestricted);
	if (keyList == NULL) {
		return;
	}

	withDigest =
			!SQLValues::TupleDigester::isOrderingAvailable(*keyList, false);
	const int32_t offset = (withDigest ? 1 : 0);

	for (SQLValues::CompColumnList::const_iterator it = keyList->begin();
			it != keyList->end(); ++it) {
		const uint32_t pos = it->getColumnPos(true);
		if (pos >= keyMap.size()) {
			keyMap.resize(pos + 1, -1);
		}
		keyMap[pos] = (entry_->inputMiddle_ ?
				static_cast<int32_t>(offset + (it - keyList->begin())) :
				static_cast<int32_t>(pos));
	}
	keyCount = static_cast<uint32_t>(
			keyMap.size() - std::count(keyMap.begin(), keyMap.end(), -1));
}

void SQLExprs::ExprRewriter::setUpMiddleFinishAggregation(
		ExprFactoryContext &cxt, Expression &expr) const {
	assert(entry_->multiStageGrouping_ && entry_->outputMiddle_);

	Expression::ModIterator exprIt(expr);
	while (exprIt.exists()) {
		exprIt.remove();
	}

	const uint32_t input = 0;

	if (entry_->multiStageKeyWithDigest_) {
		exprIt.append(createTypedIdExpr(cxt, input));
	}

	bool orderingRestricted;
	const SQLValues::CompColumnList *keyList =
			cxt.getArrangedKeyList(orderingRestricted);
	if (keyList != NULL) {
		const KeyMap &keyMap = entry_->multiStageKeyMap_;
		Usage usage(alloc_);
		usage.resize(keyMap.size(), false);

		for (SQLValues::CompColumnList::const_iterator it = keyList->begin();
				it != keyList->end(); ++it) {
			const uint32_t srcPos = it->getColumnPos(true);
			if (usage[srcPos]) {
				continue;
			}

			usage[srcPos] = true;
			const int32_t destPos = (entry_->inputMiddle_ ?
					keyMap[srcPos] : static_cast<int32_t>(srcPos));
			assert(destPos >= 0);

			Expression &outExpr =
					createColumnExpr(cxt, input, static_cast<uint32_t>(destPos));

			ExprCode &code = outExpr.getPlanningCode();
			code.setColumnType(it->getType());
			code.setAttributes(ExprCode::ATTR_ASSIGNED);

			exprIt.append(outExpr);
		}
	}

	const uint32_t aggrCount = cxt.getAggregationColumnCount();
	for (uint32_t i = 0; i < aggrCount; i++) {
		Expression &outExpr = cxt.getFactory().create(cxt, SQLType::AGG_FIRST);

		ExprCode &code = outExpr.getPlanningCode();
		code.setColumnType(cxt.getAggregationColumnType(i));
		code.setAggregationIndex(i);
		code.setAttributes(
				ExprCode::ATTR_ASSIGNED | ExprCode::ATTR_AGGREGATED);

		exprIt.append(outExpr);
	}
}

void SQLExprs::ExprRewriter::remapMultiStageGroupColumns(
		ExprFactoryContext &cxt, Expression &expr, bool forFinish) const {
	if (!entry_->multiStageGrouping_) {
		return;
	}

	ExprCode &code = expr.getPlanningCode();
	if (code.getType() == SQLType::EXPR_COLUMN) {
		if (entry_->inputMiddle_) {
			const KeyMap &keyMap = entry_->multiStageKeyMap_;
			const uint32_t srcPos = code.getColumnPos();
			const int32_t destPos =
					(srcPos < keyMap.size() ? keyMap[srcPos] : -1);
			if (destPos < 0) {
				assert(false);
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION, "");
			}
			code.setColumnPos(destPos);
		}
	}
	else if (ExprTypeUtils::isAggregation(code.getType())) {
		if (!forFinish && (entry_->inputMiddle_ || !entry_->outputMiddle_)) {
			const uint32_t input = 0;

			const uint32_t aggrIndex = code.getAggregationIndex();
			const uint32_t aggrCount = getResultCount(
					cxt.getFactory(), code.getType(),
					SQLType::AGG_PHASE_ADVANCE_PIPE);

			const uint32_t digestOffset = (entry_->inputMiddle_ &&
					entry_->multiStageKeyWithDigest_ ? 1 : 0);
			const uint32_t inCount = (entry_->inputMiddle_ ?
					entry_->multiStageKeyCount_ :
					cxt.getInputColumnCount(input));
			const uint32_t destPos = digestOffset + inCount + aggrIndex;

			Expression::ModIterator it(expr);
			while (it.exists()) {
				it.remove();
			}

			for (uint32_t i = 0; i < aggrCount; i++) {
				Expression &outExpr =
						createColumnExpr(cxt, input, destPos + i);

				ExprCode &outCode = outExpr.getPlanningCode();
				outCode.setColumnType(
						cxt.getAggregationColumnType(aggrIndex + i));
				outCode.setAttributes(ExprCode::ATTR_ASSIGNED);

				it.append(outExpr);
			}
		}
	}
	else {
		for (Expression::ModIterator it(expr); it.exists(); it.next()) {
			remapMultiStageGroupColumns(cxt, it.get(), forFinish);
		}
	}
}

bool SQLExprs::ExprRewriter::matchMultiStageGroupKey(
		const Expression &expr, bool *acceptable) const {
	if (acceptable != NULL) {
		*acceptable = false;
	}

	if (!entry_->multiStageGrouping_) {
		return false;
	}

	const ExprCode &code = expr.getCode();
	const ExprType type = code.getType();

	if (ExprTypeUtils::isAggregation(type) ||
			(code.getAttributes() & ExprCode::ATTR_AGGREGATED) != 0) {
		return false;
	}

	if (type == SQLType::EXPR_COLUMN) {
		const KeyMap &keyMap = entry_->multiStageKeyMap_;
		const uint32_t pos = code.getColumnPos();
		return (pos < keyMap.size() && keyMap[pos] >= 0);
	}

	bool matched = false;
	for (Expression::Iterator it(expr); it.exists(); it.next()) {
		bool subAcceptable = false;
		if (matchMultiStageGroupKey(it.get(), &subAcceptable)) {
			matched = true;
		}
		else if (!subAcceptable) {
			return false;
		}
	}

	if (acceptable != NULL) {
		*acceptable = true;
	}
	return matched;
}

void SQLExprs::ExprRewriter::prepareDistinctAggregation() const {
	entry_->distinctAggrMainList_.clear();
	entry_->distinctAggrSubList_.clear();
	entry_->distinctAggrsExpr_ = NULL;
	entry_->distinctAggrColumnCount_ = 0;
	entry_->distinctAggrFound_ = false;
}

void SQLExprs::ExprRewriter::replaceChildToAggrFirst(
		ExprFactoryContext &cxt, Expression::ModIterator &it) const {
	Expression &aggrExpr = cxt.getFactory().create(cxt, SQLType::AGG_FIRST);

	Expression &childExpr = it.get();
	it.remove();
	aggrExpr.addChild(&childExpr);

	ExprCode &aggrCode = aggrExpr.getPlanningCode();
	ExprCode &childCode = childExpr.getPlanningCode();

	aggrCode.setColumnType(childCode.getColumnType());
	aggrCode.setAttributes(
			ExprCode::ATTR_ASSIGNED | ExprCode::ATTR_AGGREGATED);

	it.insert(aggrExpr);
}

void SQLExprs::ExprRewriter::replaceChildToAdvanceOutput(
		ExprFactoryContext &cxt, Expression::ModIterator &it) const {
	Expression &expr = it.get();
	const ExprCode &srcCode = expr.getCode();
	assert(ExprTypeUtils::isAggregation(srcCode.getType()));

	const ExprSpec &spec = cxt.getFactory().getSpec(srcCode.getType());
	if (isDistinctAggregation(spec, srcCode.getType())) {
		bool following = false;
		for (Expression::ModIterator subIt(expr); subIt.exists(); subIt.next()) {
			const uint32_t input = 1;
			const uint32_t column = entry_->distinctAggrColumnCount_ + 1;
			const TupleColumnType type = SQLValues::TypeUtils::toNullable(
					subIt.get().getCode().getColumnType());

			{
				Expression &outExpr = createColumnExpr(cxt, input, column);

				ExprCode &outCode = outExpr.getPlanningCode();
				outCode.setColumnType(type);
				outCode.setAttributes(ExprCode::ATTR_ASSIGNED);

				subIt.remove();
				subIt.insert(outExpr);
			}

			if (following) {
				ExprCode outCode;
				outCode.setType(SQLType::EXPR_AGG_FOLLOWING);
				outCode.setColumnType(type);
				outCode.setAttributes(ExprCode::ATTR_ASSIGNED);
				Expression &outExpr = cxt.getFactory().create(cxt, outCode);

				it.next();
				it.insert(outExpr);
			}
			following = true;

			entry_->distinctAggrColumnCount_++;
		}
		return;
	}

	it.remove();
	const uint32_t aggrIndex = srcCode.getAggregationIndex();

	const uint32_t count =
			getResultCount(spec, SQLType::AGG_PHASE_ADVANCE_PIPE);
	const uint32_t baseColumn =
			static_cast<uint32_t>(entry_->distinctAggrMainList_.size()) + 1;
	for (uint32_t i = 0; i < count; i++) {
		if (i > 0) {
			it.next();
		}

		for (uint32_t j = 0; j < 2; j++) {
			if (j > 0 && !entry_->distinctAggrFound_) {
				break;
			}

			Expression *outExpr;
			if (j > 0) {
				const uint32_t input = 0;
				const uint32_t column = baseColumn + i;

				assert(aggrIndex + i + 1 == column);
				outExpr = &createColumnExpr(cxt, input, column);
			}
			else {
				outExpr = &cxt.getFactory().create(cxt, SQLType::AGG_FIRST);
			}

			ExprCode &outCode = outExpr->getPlanningCode();
			outCode.setColumnType(
					cxt.getAggregationColumnType(aggrIndex + i));

			if (j > 0) {
				outCode.setAttributes(ExprCode::ATTR_ASSIGNED);
			}
			else {
				outCode.setAggregationIndex(aggrIndex + i);
				outCode.setAttributes(
						ExprCode::ATTR_ASSIGNED | ExprCode::ATTR_AGGREGATED);
			}

			if (j == 0 && entry_->distinctAggrFound_) {
				entry_->distinctAggrMainList_.push_back(outExpr);
			}
			else {
				it.insert(*outExpr);
			}
		}
	}
}

void SQLExprs::ExprRewriter::replaceChildToDistinctFinishOutput(
		ExprFactoryContext &cxt, Expression::ModIterator &it) const {
	assert(entry_->distinctAggrFound_);

	Expression &expr = it.get();
	const ExprCode &code = expr.getCode();
	assert(ExprTypeUtils::isAggregation(code.getType()));

	const bool distinct =
			isDistinctAggregation(cxt.getFactory(), code.getType());

	if (distinct) {
		for (Expression::ModIterator subIt(expr); subIt.exists(); subIt.next()) {
			const uint32_t input =  1;
			const uint32_t column = entry_->distinctAggrColumnCount_ + 1;
			const TupleColumnType type = SQLValues::TypeUtils::toNullable(
					subIt.get().getCode().getColumnType());

			Expression &outExpr = createColumnExpr(cxt, input, column);

			ExprCode &outCode = outExpr.getPlanningCode();
			outCode.setColumnType(type);
			outCode.setAttributes(ExprCode::ATTR_ASSIGNED);

			subIt.remove();
			subIt.insert(outExpr);

			entry_->distinctAggrColumnCount_++;
		}
	}
	else {
		it.remove();

		const uint32_t input = 0;
		const uint32_t column = static_cast<uint32_t>(
				entry_->distinctAggrMainList_.size()) + 1;

		Expression &outExpr = createColumnExpr(cxt, input, column);

		ExprCode &outCode = outExpr.getPlanningCode();
		outCode.setColumnType(code.getColumnType());
		outCode.setAttributes(ExprCode::ATTR_ASSIGNED);

		it.insert(outExpr);
		entry_->distinctAggrMainList_.push_back(&expr);
	}
}

void SQLExprs::ExprRewriter::replaceChildToNull(
		ExprFactoryContext &cxt, Expression::ModIterator &it) const {
	Expression &nullExpr = createConstExpr(cxt, TupleValue());
	Expression &childExpr = it.get();

	ExprCode &nullCode = nullExpr.getPlanningCode();
	ExprCode &childCode = childExpr.getPlanningCode();

	nullCode.setColumnType(childCode.getColumnType());
	nullCode.setAttributes(childCode.getAttributes());

	it.remove();
	it.insert(nullExpr);
}

SQLExprs::Expression& SQLExprs::ExprRewriter::replaceParentToPlanning(
		ExprFactoryContext &cxt, Expression &parentExpr) const {
	ExprFactoryContext::Scope scope(cxt);
	cxt.setPlanning(true);

	Expression &dest = cxt.getFactory().create(cxt, parentExpr.getCode());
	Expression::ModIterator destIt(dest);
	for (Expression::ModIterator it(parentExpr); it.exists();) {
		Expression &sub = it.get();
		it.remove();
		destIt.append(sub);
	}

	return dest;
}

void SQLExprs::ExprRewriter::setAggregationArranged(Expression &expr) {
	expr.getPlanningCode().setAttributes(
			(expr.getCode().getAttributes() | ExprCode::ATTR_AGGR_ARRANGED));
}


SQLExprs::ExprRewriter::Scope::Scope(ExprRewriter &rewriter) :
		rewriter_(rewriter),
		prevEntry_(rewriter_.entry_),
		localEntry_(*prevEntry_) {
	rewriter_.entry_ = &localEntry_;
}

SQLExprs::ExprRewriter::Scope::~Scope() {
	rewriter_.entry_ = prevEntry_;
}


SQLExprs::ExprRewriter::ScopedEntry::ScopedEntry(util::StackAllocator &alloc) :
		columnMapping_(false),
		inputProjected_(false),
		inputNullProjected_(false),
		idProjected_(false),
		columnMapUpdated_(false),
		columnMap_(alloc),
		idColumnMap_(alloc),
		inputMap_(alloc),
		inputUsage_(alloc),
		columnUsage_(alloc),
		idUsage_(alloc),
		inputNullList_(alloc),
		codeSetUpAlways_(false),
		multiStageGrouping_(),
		inputMiddle_(false),
		outputMiddle_(false),
		multiStageKeyWithDigest_(false),
		multiStageKeyMap_(alloc),
		multiStageKeyCount_(0),
		distinctAggrMainList_(alloc),
		distinctAggrSubList_(alloc),
		distinctAggrsExpr_(NULL),
		distinctAggrColumnCount_(0),
		distinctAggrFound_(false) {
}


SQLExprs::SyntaxExprRewriter::SyntaxExprRewriter(
		SQLValues::ValueContext &valueCxt,
		const util::Vector<TupleValue> *parameterList) :
		valueCxt_(valueCxt),
		parameterList_(parameterList) {
}

SQLExprs::Expression* SQLExprs::SyntaxExprRewriter::toExpr(
		const SyntaxExpr *src) const {
	if (src == NULL) {
		return NULL;
	}

	ExprFactoryContext cxt(valueCxt_.getAllocator());
	Expression::InOption option(valueCxt_);
	option.syntaxExpr_ = src;
	option.parameterList_ = parameterList_;
	return &Expression::importFrom(cxt, option);
}

SQLExprs::Expression& SQLExprs::SyntaxExprRewriter::toColumnList(
		const SyntaxExpr *src, bool columnSingle) const {
	ExprFactoryContext cxt(valueCxt_.getAllocator());
	Expression &expr = cxt.getFactory().create(cxt, SQLType::EXPR_LIST);

	Expression::ModIterator destIt(expr);
	if (src != NULL){
		Expression *sub = toColumnListElement(toExpr(src), columnSingle);
		if (sub != NULL) {
			destIt.append(*sub);
		}
	}

	return expr;
}

SQLExprs::Expression& SQLExprs::SyntaxExprRewriter::toColumnList(
		const SyntaxExprList &src, bool columnSingle) const {
	ExprFactoryContext cxt(valueCxt_.getAllocator());
	Expression &expr = cxt.getFactory().create(cxt, SQLType::EXPR_LIST);

	Expression::ModIterator destIt(expr);
	for (SyntaxExprList::const_iterator it = src.begin(); it != src.end(); ++it) {
		Expression *sub = toColumnListElement(toExpr(&(*it)), columnSingle);
		if (sub != NULL) {
			destIt.append(*sub);
		}
	}

	return expr;
}

SQLExprs::Expression& SQLExprs::SyntaxExprRewriter::toProjection(
		const SyntaxExprList &src) const {
	return makeExpr(SQLType::EXPR_PROJECTION, &src);
}

SQLExprs::Expression& SQLExprs::SyntaxExprRewriter::toSelection(
		const SyntaxExprList &srcProj, const SyntaxExpr *srcPred) const {
	ExprFactoryContext cxt(valueCxt_.getAllocator());
	Expression &expr = cxt.getFactory().create(cxt, SQLType::EXPR_SELECTION);

	Expression::ModIterator destIt(expr);

	{
		Expression &production =
				cxt.getFactory().create(cxt, SQLType::EXPR_PRODUCTION);
		if (srcPred != NULL) {
			production.addChild(toExpr(srcPred));
		}
		destIt.append(production);
	}

	destIt.append(toProjection(srcProj));

	return expr;
}

SQLExprs::SyntaxExpr* SQLExprs::SyntaxExprRewriter::toSyntaxExpr(
		const Expression &src) const {
	Expression::OutOption option(
			&valueCxt_.getAllocator(), ExprFactory::getDefaultFactory());

	SQLExprs::SyntaxExpr *dest = NULL;
	option.syntaxExprRef_ = &dest;

	src.exportTo(option);
	assert(dest != NULL);

	return dest;
}

SQLExprs::SyntaxExprList SQLExprs::SyntaxExprRewriter::toSyntaxExprList(
		const Expression &src) const {
	Expression::OutOption option(
			&valueCxt_.getAllocator(), ExprFactory::getDefaultFactory());

	SyntaxExprList dest(valueCxt_.getAllocator());
	for (Expression::Iterator it(src); it.exists(); it.next()) {
		SQLExprs::SyntaxExpr *destElem = NULL;
		option.syntaxExprRef_ = &destElem;

		src.exportTo(option);
		assert(destElem != NULL);

		dest.push_back(*destElem);
	}

	return dest;
}

SQLExprs::Expression& SQLExprs::SyntaxExprRewriter::makeExpr(
		ExprType type, const SyntaxExprList *argList) const {
	ExprFactoryContext cxt(valueCxt_.getAllocator());
	Expression &expr = cxt.getFactory().create(cxt, type);

	if (argList != NULL) {
		Expression::ModIterator destIt(expr);
		for (SyntaxExprList::const_iterator it = argList->begin();
				it != argList->end(); ++it) {
			destIt.append(*toExpr(&(*it)));
		}
	}

	return expr;
}

bool SQLExprs::SyntaxExprRewriter::findAdjustedRangeValues(
		const SyntaxExpr &expr,
		const util::Vector<TupleColumnType> &columnTypeList,
		const util::Vector<TupleValue> *parameterList,
		SQLExprs::IndexCondition &cond) const {
	cond = SQLExprs::IndexCondition();

	assert(!columnTypeList.empty());

	TupleList::Info tupleInfo;
	tupleInfo.columnCount_ = columnTypeList.size();
	tupleInfo.columnTypeList_ = &columnTypeList[0];

	const uint32_t indexType = (
			(1 << SQLType::INDEX_TREE_RANGE) |
			(1 << SQLType::INDEX_TREE_EQ));

	Expression::InOption option(valueCxt_);
	option.syntaxExpr_ = &expr;
	option.parameterList_ = parameterList;

	ExprFactoryContext factoryCxt(valueCxt_.getAllocator());
	Expression *localExpr = &Expression::importFrom(factoryCxt, option);
	if (localExpr == NULL) {
		return false;
	}

	switch (localExpr->getCode().getType()) {
	case SQLType::OP_EQ:
	case SQLType::OP_LT:
	case SQLType::OP_GT:
	case SQLType::OP_LE:
	case SQLType::OP_GE:
		break;
	default:
		return false;
	}

	Expression::Iterator it(*localExpr);
	const Expression *columnExpr = &it.get();
	if (columnExpr->getCode().getType() != SQLType::EXPR_COLUMN) {
		it.next();
		columnExpr = &it.get();
	}

	if (columnExpr->getCode().getType() != SQLType::EXPR_COLUMN) {
		return false;
	}

	IndexSelector selector(valueCxt_, SQLType::EXPR_COLUMN, tupleInfo);
	selector.addIndex(columnExpr->getCode().getColumnPos(), indexType);
	selector.completeIndex();

	selector.select(*localExpr);
	const IndexConditionList &condList = selector.getConditionList();
	if (!selector.isSelected()) {
		return false;
	}

	cond = condList.front();
	return true;
}

TupleValue SQLExprs::SyntaxExprRewriter::evalConstExpr(
		ExprContext &cxt, const ExprFactory &factory,
		const SyntaxExpr &expr) {
	util::StackAllocator &alloc = cxt.getAllocator();
	util::StackAllocator::Scope allocScope(alloc);

	SyntaxExprRewriter syntaxRewriter(cxt.getValueContext(), NULL);
	Expression *planningExpr = syntaxRewriter.toExpr(&expr);
	assert(planningExpr != NULL);

	ExprFactoryContext factoryCxt(alloc);
	factoryCxt.setFactory(&factory);
	factoryCxt.setPlanning(false);
	factoryCxt.setAggregationPhase(true, SQLType::AGG_PHASE_ALL_PIPE);

	ExprRewriter rewriter(alloc);
	const Expression &evaluableExpr =
			rewriter.rewrite(factoryCxt, *planningExpr, NULL);

	return ExprRewriter::evalConstExpr(cxt, evaluableExpr);
}

void SQLExprs::SyntaxExprRewriter::checkExprArgs(
		ExprContext &cxt, const ExprFactory &factory,
		const SyntaxExpr &expr) {
	util::StackAllocator &alloc = cxt.getAllocator();
	util::StackAllocator::Scope allocScope(alloc);

	SyntaxExprRewriter syntaxRewriter(cxt.getValueContext(), NULL);
	Expression *planningExpr = syntaxRewriter.toExpr(&expr);
	assert(planningExpr != NULL);

	const ExprSpec &spec = factory.getSpec(planningExpr->getCode().getType());

	ExprFactoryContext factoryCxt(alloc);
	factoryCxt.setFactory(&factory);

	size_t argIndex = 0;
	for (Expression::ModIterator it(*planningExpr); it.exists(); it.next()) {
		const ExprCode &code = it.get().getCode();
		const TupleColumnType valueType = code.getValue().getType();

		const ExprSpec::In *argIn = (argIndex < ExprSpec::IN_LIST_SIZE ?
				&spec.inList_[argIndex] : NULL);

		const bool numericPromo = (argIn != NULL &&
				ExprSpecBase::InPromotionVariants::isNumericPromotion(*argIn));
		const TupleColumnType nullType = TupleTypes::TYPE_NULL;
		const TupleColumnType longType = TupleTypes::TYPE_LONG;

		const TupleColumnType argType = (argIn == NULL ?
				nullType : (numericPromo ? longType : argIn->typeList_[0]));

		if (SQLValues::TypeUtils::isNull(argType)) {
			while (it.exists()) {
				it.remove();
			}
			break;
		}
		if (code.getType() != SQLType::EXPR_CONSTANT || valueType != argType) {
			it.remove();
			it.insert(ExprRewriter::createEmptyConstExpr(factoryCxt, argType));
		}
		argIndex++;
	}

	factoryCxt.setPlanning(false);
	factoryCxt.setArgChecking(true);
	factoryCxt.setAggregationPhase(true, SQLType::AGG_PHASE_ALL_PIPE);

	ExprRewriter rewriter(alloc);
	const Expression &evaluableExpr =
			rewriter.rewrite(factoryCxt, *planningExpr, NULL);

	SQLValues::VarContext::Scope varScope(
			cxt.getValueContext().getVarContext());
	evaluableExpr.eval(cxt);
}

SQLExprs::Expression* SQLExprs::SyntaxExprRewriter::toColumnListElement(
		Expression *src, bool columnSingle) {
	if (src == NULL) {
		return src;
	}

	ExprCode &code = src->getPlanningCode();
	do {
		const ExprType exprType = code.getType();
		if (exprType == SQLType::EXPR_RANGE_GROUP) {
			if (src->child().getCode().getType() != SQLType::EXPR_COLUMN) {
				break;
			}
			Expression::ModIterator it(*src);
			Expression &dest =  it.get();
			it.remove();
			return toColumnListElement(&dest, columnSingle);
		}

		if (columnSingle) {
			if (exprType != SQLType::EXPR_COLUMN) {
				break;
			}
			if (code.getInput() != 0) {
				break;
			}
		}
		else {
			if (exprType == SQLType::EXPR_CONSTANT) {
				if (!SQLValues::ValueUtils::isTrue(code.getValue())) {
					break;
				}
				return NULL;
			}

			if (!ExprTypeUtils::isCompOp(exprType, false) ||
					src->getChildCount() != 2) {
				break;
			}

			const Expression *left = &src->child();
			const Expression *right = &left->next();
			if (left->getCode().getInput() != 0) {
				std::swap(left, right);
				if (left->getCode().getInput() != 0) {
					break;
				}

				code.setType(ExprTypeUtils::swapCompOp(code.getType()));

				Expression::ModIterator it(*src);
				Expression *orgLeft = &it.get();
				it.remove();
				it.next();
				it.append(*orgLeft);

				assert(&src->child() == left);
				assert(&src->child().next() == right);
			}

			if (left->getCode().getInput() != 0 ||
					right->getCode().getInput() != 1) {
				break;
			}
		}
		return src;
	}
	while (false);

	GS_THROW_USER_ERROR(
			GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION, "");
}


SQLExprs::TypeResolver::TypeResolver(
		const ExprSpec &spec, AggregationPhase aggrPhase, bool grouping) :
		spec_(spec),
		aggrPhase_(aggrPhase),
		grouping_(grouping),
		flags_(resolveSpecFlags(spec, aggrPhase)),
		nextIndex_(0),
		lastType_(TupleTypes::TYPE_NULL),
		promotedType_(TupleTypes::TYPE_NULL),
		inNullable_(false),
		conversionError_(-1),
		promotionError_(-1) {
	assert(aggrPhase == SQLType::AGG_PHASE_ALL_PIPE ||
			aggrPhase == SQLType::AGG_PHASE_ADVANCE_PIPE ||
			aggrPhase == SQLType::AGG_PHASE_MERGE_PIPE);
	std::fill(
			inheritanceList_, inheritanceList_ + INHERITANCE_LIST_SIZE,
			static_cast<TupleColumnType>(TupleTypes::TYPE_NULL));
}

void SQLExprs::TypeResolver::next(TupleColumnType type) {
	if (nextIndex_ > 0) {
		acceptLast(false);
	}

	lastType_ = type;
	nextIndex_++;
}

SQLExprs::TypeResolver::ResultInfo
SQLExprs::TypeResolver::complete(bool checking) {
	if (nextIndex_ > 0) {
		acceptLast(true);
	}

	do {
		if (isError()) {
			break;
		}

		const int32_t argCount = static_cast<int32_t>(nextIndex_);
		{
			const int32_t min = getMinArgCount();
			const int32_t max = getMaxArgCount();
			if (min > argCount || (max < argCount && max >= 0)) {
				break;
			}
		}

		if (!TypeUtils::isNull(promotedType_) && !convertPromotedType()) {
			break;
		}

		TupleColumnType baseType1;
		if ((flags_ &
				(ExprSpec::FLAG_INHERIT1 | ExprSpec::FLAG_INHERIT2)) == 0) {
			baseType1 = findResult(0);
		}
		else {
			const size_t index =
					(flags_ & ExprSpec::FLAG_INHERIT1) != 0 ? 0 : 1;
			const ExprSpec::In *in = findIn(index, false);
			assert(in != NULL);

			if ((getInFlags(*in) & ExprSpec::FLAG_PROMOTABLE) != 0) {
				baseType1 = promotedType_;
			}
			else {
				baseType1 = inheritanceList_[index];
			}
		}

		bool nullable1;
		if ((flags_ & ExprSpec::FLAG_NON_NULLABLE) != 0 ||
				isResultNonNullable(0)) {
			nullable1 = false;
		}
		else if ((flags_ & ExprSpec::FLAG_NULLABLE) != 0) {
			nullable1 = true;
		}
		else if (argCount % 2 == 0 &&
				(flags_ & ExprSpec::FLAG_EVEN_ARGS_NULLABLE) != 0) {
			nullable1 = true;
		}
		else if ((flags_ & ExprSpec::FLAG_INHERIT_NULLABLE1) != 0) {
			if ((getInFlags(*findIn(0, false)) &
					ExprSpec::FLAG_PROMOTABLE) == 0) {
				nullable1 = TypeUtils::isNullable(inheritanceList_[0]);
			}
			else {
				nullable1 = TypeUtils::isNullable(promotedType_);
			}
		}
		else if (spec_.isAggregation() && (!grouping_ || isAdvanceAggregation())) {
			nullable1 = true;
		}
		else {
			nullable1 = inNullable_;
		}

		const bool advDistinct = (isAdvanceAggregation() && isDistinctAggregation());

		TupleColumnType alterType = TupleTypes::TYPE_NULL;
		if (!advDistinct && hasInheritableResult()) {
			for (size_t i = 0;; i++) {
				const ExprSpec::In *in =
						(i < INHERITANCE_LIST_SIZE ? findIn(i, false) : NULL);
				if (in == NULL || !TypeUtils::isDeclarable(in->typeList_[0])) {
					const TupleColumnType inheritedType =
							inheritanceList_[(in == NULL ? 0 : i)];
					if (!TypeUtils::isNull(inheritedType)) {
						alterType = inheritedType;
					}
					else {
						alterType = promotedType_;
					}
					break;
				}
			}

			if ((spec_.flags_ & (
					ExprSpec::FLAG_WINDOW_ONLY |
					ExprSpec::FLAG_PSEUDO_WINDOW)) == 0 ||
					((spec_.flags_ & ExprSpec::FLAG_WINDOW_ONLY) != 0 &&
							isAdvanceAggregation()))
			{
				alterType = TypeUtils::toNullable(alterType);
			}
		}

		ResultInfo retInfo;
		TupleColumnType *const retBegin = retInfo.typeList_;
		TupleColumnType *const retEnd = retBegin +
				sizeof(retInfo.typeList_) / sizeof(*retInfo.typeList_);

		for (TupleColumnType *retIt = retBegin; retIt != retEnd; ++retIt) {
			const size_t index = static_cast<size_t>(retIt - retBegin);
			const TupleColumnType srcType = findResult(index);
			if (retIt != retBegin && TypeUtils::isNull(srcType)) {
				break;
			}

			if (advDistinct) {
				if (index >= INHERITANCE_LIST_SIZE) {
					retInfo = ResultInfo();
					break;
				}
				*retIt = TypeUtils::toNullable(inheritanceList_[index]);
			}
			else if (TypeUtils::isAny(srcType) || (!TypeUtils::isNull(srcType) &&
					!TypeUtils::isDeclarable(srcType))) {
				*retIt = alterType;
			}
			else {
				if (retIt == retBegin) {
					*retIt = TypeUtils::setNullable(baseType1, nullable1);
				}
				else {
					*retIt = srcType;
					if ((flags_ & ExprSpec::FLAG_NULLABLE) != 0 ||
							!isResultNonNullable(index)) {
						*retIt = TypeUtils::setNullable(*retIt, true);
					}
				}
			}

			if (TypeUtils::isNull(*retIt)) {
				retInfo = ResultInfo();
				break;
			}
		}

		if (TypeUtils::isNull(*retBegin)) {
			break;
		}

		return retInfo;
	}
	while (false);

	if (checking) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION, "");
	}

	return ResultInfo();
}

int32_t SQLExprs::TypeResolver::getMinArgCount() const {
	if (isMergeAggregation()) {
		for (int32_t i = 0;; i++) {
			if (i >= ExprSpec::AGGR_LIST_SIZE ||
					TypeUtils::isNull(spec_.aggrList_[i].typeList_[0])) {
				assert(i > 0);
				return i;
			}
		}
	}
	return spec_.argCounts_.first;
}

int32_t SQLExprs::TypeResolver::getMaxArgCount() const {
	if (isMergeAggregation()) {
		return getMinArgCount();
	}
	return spec_.argCounts_.second;
}

uint32_t SQLExprs::TypeResolver::getResultCount() const {
	for (uint32_t count = 1;; ++count) {
		if (TypeUtils::isNull(findResult(count))) {
			return count;
		}
	}
}

bool SQLExprs::TypeResolver::isError() const {
	return (conversionError_ >= 0 || promotionError_ >= 0);
}

bool SQLExprs::TypeResolver::convertPromotedType() {
	size_t inListSize;
	const ExprSpec::In *inList = getInList(inListSize);

	for (size_t index = 0; index < inListSize; index++) {
		const ExprSpec::In *in = &inList[index];
		if (TypeUtils::isNull(in->typeList_[0])) {
			break;
		}

		if ((getInFlags(*in) & ExprSpec::FLAG_PROMOTABLE) == 0) {
			continue;
		}

		bool found = false;
		for (size_t i = 0; i < ExprSpec::IN_TYPE_COUNT; i++) {
			if (i <= 0) {
				continue;
			}
			if (!TypeUtils::isNull(in->typeList_[i]) &&
					TypeUtils::isAny(promotedType_)) {
				found = true;
				break;
			}
		}
		if (found) {
			continue;
		}

		if (ExprSpecBase::InPromotionVariants::isTimestampPromotion(*in) &&
				!TypeUtils::isNull(TypeUtils::findConversionType(
						promotedType_,
						TypeUtils::toNullable(in->typeList_[0]), true, true))) {
			continue;
		}

		for (size_t i = 0; i < ExprSpec::IN_TYPE_COUNT; i++) {
			const TupleColumnType converted = TypeUtils::findConversionType(
					promotedType_,
					TypeUtils::toNullable(in->typeList_[i]), true, true);
			if (!TypeUtils::isNull(converted)) {
				promotedType_ = converted;
				found = true;
				break;
			}
		}
		if (found) {
			continue;
		}

		return false;
	}

	return true;
}

void SQLExprs::TypeResolver::acceptLast(bool tail) {
	assert(nextIndex_ > 0);
	const size_t index = nextIndex_ - 1;

	const TupleColumnType type = lastType_;
	lastType_ = TupleTypes::TYPE_NULL;

	const ExprSpec::In *in = findIn(index, tail);
	if (in == NULL) {
		return;
	}

	if (TypeUtils::isNull(type)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_EXPRESSION, "");
	}

	const bool nullable =
			(TypeUtils::isNullable(type) || TypeUtils::isAny(type));
	inNullable_ |= nullable;

	const uint32_t inFlags = getInFlags(*in);
	const TupleColumnType baseType = TypeUtils::toNonNullable(type);
	TupleColumnType converted;
	do {
		if ((inFlags & ExprSpec::FLAG_EXACT) != 0 &&
				!TypeUtils::isAny(in->typeList_[0])) {
			bool exactMatched = false;
			for (size_t i = 0; i < ExprSpec::IN_TYPE_COUNT; i++) {
				if (type == in->typeList_[i]) {
					exactMatched = true;
					break;
				}
			}
			if (!exactMatched) {
				conversionError_ = static_cast<int32_t>(index);
				return;
			}

			converted = type;
			break;
		}

		if ((inFlags & ExprSpec::FLAG_PROMOTABLE) != 0) {
			if (TypeUtils::isNull(promotedType_)) {
				promotedType_ = type;
			}
			else {
				promotedType_ = TypeUtils::findPromotionType(
						promotedType_, type, true);
				if (TypeUtils::isNull(promotedType_)) {
					promotionError_ = static_cast<int32_t>(index);
					return;
				}
			}

			converted = TupleTypes::TYPE_NULL;
			break;
		}

		for (size_t i = 0; i < ExprSpec::IN_TYPE_COUNT; i++) {
			const TupleColumnType desired =
					TypeUtils::toNullable(in->typeList_[i]);

			converted = TypeUtils::findConversionType(
					baseType, desired, true, true);
			if (!TypeUtils::isNull(converted)) {
				break;
			}
		}

		if (TypeUtils::isNull(converted)) {
			conversionError_ = static_cast<int32_t>(index);
			return;
		}
	}
	while (false);

	if (isAdvanceAggregation() && isDistinctAggregation()) {
		converted = TypeUtils::toNullable(type);
	}

	if (index < INHERITANCE_LIST_SIZE) {
		inheritanceList_[index] =
				TypeUtils::setNullable(converted, nullable);
	}
}

const SQLExprs::ExprSpec::In*
SQLExprs::TypeResolver::findIn(size_t index, bool tail) const {
	if ((flags_ & ExprSpec::FLAG_ODD_TAIL) != 0 && tail && index % 2 == 0) {
		return findLast();
	}

	size_t inListSize;
	const ExprSpec::In *inList = getInList(inListSize);
	if (index < inListSize && !TypeUtils::isNull(inList[index].typeList_[0])) {
		return &inList[index];
	}
	else if ((flags_ & ExprSpec::FLAG_REPEAT1) != 0) {
		return findLast();
	}
	else if ((flags_ & ExprSpec::FLAG_REPEAT2) != 0) {
		const ExprSpec::In *last = findLast();
		if (last != NULL) {
			const ptrdiff_t count = last - inList;
			if ((index - count) % 2 == 1) {
				--last;
			}
			return last;
		}
	}

	return NULL;
}

const SQLExprs::ExprSpec::In* SQLExprs::TypeResolver::findLast() const {
	size_t inListSize;
	const ExprSpec::In *inList = getInList(inListSize);
	for (size_t index = inListSize; index > 0;) {
		const ExprSpec::In &in = inList[--index];

		if (!TypeUtils::isNull(in.typeList_[0])) {
			return &in;
		}
	}

	return NULL;
}

bool SQLExprs::TypeResolver::hasInheritableResult() const {
	if (!spec_.isAggregation()) {
		return false;
	}
	for (size_t index = 0;; index++) {
		const TupleColumnType type = findResult(index);
		if (TypeUtils::isNull(type)) {
			return false;
		}
		else if (!TypeUtils::isDeclarable(type)) {
			return true;
		}
	}
}

const SQLExprs::ExprSpec::In* SQLExprs::TypeResolver::getInList(
		size_t &size) const {
	if (isMergeAggregation()) {
		size = ExprSpec::AGGR_LIST_SIZE;
		return spec_.aggrList_;
	}
	else {
		size = ExprSpec::IN_LIST_SIZE;
		return spec_.inList_;
	}
}

TupleColumnType SQLExprs::TypeResolver::findResult(size_t index) const {
	if (isAdvanceAggregation()) {
		const ExprSpec::In *inList;
		size_t size;
		if (isDistinctAggregation()) {
			inList = spec_.inList_;
			size = ExprSpec::IN_LIST_SIZE;
		}
		else {
			inList = spec_.aggrList_;
			size = ExprSpec::AGGR_LIST_SIZE;
		}
		if (index < size) {
			return inList[index].typeList_[0];
		}
	}
	else {
		if (index == 0) {
			return spec_.outType_;
		}
	}
	return TupleTypes::TYPE_NULL;
}

bool SQLExprs::TypeResolver::isResultNonNullable(size_t index) const {
	return (isAdvanceAggregation() && !isDistinctAggregation() &&
			index < ExprSpec::AGGR_LIST_SIZE &&
			(spec_.aggrList_[index].flags_ & ExprSpec::FLAG_EXACT) != 0);
}

uint32_t SQLExprs::TypeResolver::getInFlags(const ExprSpec::In &in) const {
	if (isMergeAggregation() && TypeUtils::isNull(in.typeList_[0])) {
		return ExprSpec::FLAG_EXACT;
	}
	return in.flags_;
}

uint32_t SQLExprs::TypeResolver::resolveSpecFlags(
		const ExprSpec &spec, AggregationPhase aggrPhase) {
	if (spec.isAggregation() &&
			aggrPhase == SQLType::AGG_PHASE_ADVANCE_PIPE) {
		if ((spec.flags_ & ExprSpec::FLAG_DISTINCT) == 0) {
			return 0;
		}
		else {
			return ExprSpec::FLAG_NULLABLE | ExprSpec::FLAG_INHERIT1;
		}
	}
	return spec.flags_;
}

bool SQLExprs::TypeResolver::isAdvanceAggregation() const {
	return (spec_.isAggregation() &&
			aggrPhase_ == SQLType::AGG_PHASE_ADVANCE_PIPE);
}

bool SQLExprs::TypeResolver::isMergeAggregation() const {
	return (spec_.isAggregation() && !isDistinctAggregation() &&
			aggrPhase_ == SQLType::AGG_PHASE_MERGE_PIPE);
}

bool SQLExprs::TypeResolver::isDistinctAggregation() const {
	return (spec_.isAggregation() &&
			(spec_.flags_ & ExprSpec::FLAG_DISTINCT) != 0);
}


SQLExprs::TypeResolverResult::TypeResolverResult() {
	std::fill(
			typeList_, typeList_ + sizeof(typeList_) / sizeof(*typeList_),
			static_cast<TupleColumnType>(TupleTypes::TYPE_NULL));
}


SQLExprs::IndexSpec::IndexSpec() :
		columnList_(NULL),
		columnCount_(0),
		indexFlags_(0),
		available_(false) {
}

bool SQLExprs::IndexSpec::operator==(const IndexSpec &another) const {
	return compareTo(another) == 0;
}

bool SQLExprs::IndexSpec::operator<(const IndexSpec &another) const {
	return compareTo(another) < 0;
}

int32_t SQLExprs::IndexSpec::compareTo(const IndexSpec &another) const {
	{
		const int32_t comp = SQLValues::ValueUtils::compareRawValue(
				columnCount_, another.columnCount_);
		if (comp != 0) {
			return comp;
		}
	}

	for (size_t i = 0; i < columnCount_; i++) {
		const int32_t comp = SQLValues::ValueUtils::compareRawValue(
				columnList_[i], another.columnList_[i]);
		if (comp != 0) {
			return comp;
		}
	}

	return 0;
}

size_t SQLExprs::IndexSpec::getColumnCount() const {
	return columnCount_;
}

uint32_t SQLExprs::IndexSpec::getColumn(size_t index) const {
	assert(index <= columnCount_);
	return columnList_[index];
}

SQLExprs::IndexSpec::ColumnsIterator
SQLExprs::IndexSpec::columnsBegin() const {
	return columnList_;
}

SQLExprs::IndexSpec::ColumnsIterator SQLExprs::IndexSpec::columnsEnd() const {
	return columnList_ + columnCount_;
}


const uint32_t SQLExprs::IndexCondition::EMPTY_IN_COLUMN =
		std::numeric_limits<uint32_t>::max();

SQLExprs::IndexCondition::IndexCondition() :
		column_(EMPTY_IN_COLUMN),
		opType_(SQLType::START_EXPR),
		inColumnPair_(EMPTY_IN_COLUMN, EMPTY_IN_COLUMN),
		andCount_(1),
		andOrdinal_(0),
		compositeAndCount_(1),
		compositeAndOrdinal_(0),
		bulkOrCount_(1),
		bulkOrOrdinal_(0),
		specId_(0),
		firstHitOnly_(false),
		descending_(false) {
}

SQLExprs::IndexCondition SQLExprs::IndexCondition::createNull() {
	IndexCondition cond;
	cond.opType_ = SQLType::EXPR_CONSTANT;
	return cond;
}

SQLExprs::IndexCondition SQLExprs::IndexCondition::createBool(bool value) {
	IndexCondition cond;
	cond.opType_ = SQLType::EXPR_CONSTANT;
	cond.valuePair_.first = SQLValues::ValueUtils::toAnyByBool(value);
	return cond;
}

bool SQLExprs::IndexCondition::isEmpty() const {
	return opType_ == SQLType::START_EXPR;
}

bool SQLExprs::IndexCondition::isNull() const {
	return opType_ == SQLType::EXPR_CONSTANT &&
			SQLValues::ValueUtils::isNull(valuePair_.first);
}

bool SQLExprs::IndexCondition::equals(bool value) const {
	if (opType_ != SQLType::EXPR_CONSTANT) {
		return false;
	}

	if (value) {
		return SQLValues::ValueUtils::isTrue(valuePair_.first);
	}
	else {
		return SQLValues::ValueUtils::isFalse(valuePair_.first);
	}
}

bool SQLExprs::IndexCondition::isStatic() const {
	return isEmpty() || opType_ == SQLType::EXPR_CONSTANT;
}

bool SQLExprs::IndexCondition::isAndTop() const {
	return andOrdinal_ == 0;
}

bool SQLExprs::IndexCondition::isBulkTop() const {
	return (bulkOrOrdinal_ == 0 && isAndTop());
}

bool SQLExprs::IndexCondition::isBinded() const {
	return inColumnPair_.first == EMPTY_IN_COLUMN &&
			inColumnPair_.second == EMPTY_IN_COLUMN;
}


SQLExprs::IndexSelector::IndexSelector(
		SQLValues::ValueContext &valueCxt, ExprType columnExprType,
		const TupleList::Info &info) :
		stdAlloc_(valueCxt.getStdAllocator()),
		varCxt_(valueCxt.getVarContext()),
		localValuesHolder_(valueCxt),
		columnExprType_(columnExprType),
		indexList_(
				info.columnCount_, IndexFlagsEntry(), valueCxt.getAllocator()),
		allColumnsList_(stdAlloc_),
		tupleInfo_(
				info.columnTypeList_,
				info.columnTypeList_ + info.columnCount_,
				valueCxt.getAllocator()),
		condList_(valueCxt.getAllocator()),
		indexMatchList_(valueCxt.getAllocator()),
		condOrdinalList_(valueCxt.getAllocator()),
		specList_(stdAlloc_),
		columnSpecIdList_(stdAlloc_),
		indexAvailable_(true),
		placeholderAffected_(false),
		multiAndConditionEnabled_(false),
		bulkGrouping_(false),
		complexReordering_(false),
		completed_(false),
		cleared_(false),
		exprFactory_(&ExprFactory::getDefaultFactory()) {
}

SQLExprs::IndexSelector::~IndexSelector() {
	clearIndexColumns();
}

void SQLExprs::IndexSelector::clearIndex() {
	completed_ = false;
	cleared_ = true;

	indexList_.assign(indexList_.size(), IndexFlagsEntry());

	for (SpecList::iterator it = specList_.begin(); it != specList_.end(); ++it) {
		it->available_ = false;
	}
}

void SQLExprs::IndexSelector::addIndex(
		uint32_t column, IndexFlags flags) {
	util::Vector<uint32_t> columnList(1, column, indexList_.get_allocator());
	util::Vector<IndexFlags> flagsList(1, flags, indexList_.get_allocator());
	addIndex(columnList, flagsList);
}

void SQLExprs::IndexSelector::addIndex(
		const util::Vector<uint32_t> &columnList,
		const util::Vector<IndexFlags> &flagsList) {
	if (completed_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	assert(columnList.size() == flagsList.size());

	const bool composite = (columnList.size() > 1);

	if (composite) {
		IndexSpec spec = createIndexSpec(columnList);
		spec.available_ = true;

		if (cleared_) {
			SpecList::iterator it = std::lower_bound(
					specList_.begin(), specList_.end(), spec);
			if (it == specList_.end() || !(*it == spec)) {
				return;
			}
			it->available_ = true;
		}
		else {
			specList_.push_back(spec);
		}
	}

	util::Vector<IndexFlags>::const_iterator flagsIt = flagsList.begin();
	for (util::Vector<uint32_t>::const_iterator it = columnList.begin();
			it != columnList.end(); ++it, ++flagsIt) {
		const uint32_t column = *it;
		const IndexFlags flags = *flagsIt;

		IndexFlagsEntry &flagsEntry = indexList_[column];
		flagsEntry.add(
				(composite ?
						IndexFlagsEntry::FLAGS_TYPE_COMPOSITE_ALL :
						IndexFlagsEntry::FLAGS_TYPE_SINGLE),
				flags);

		if (composite && it == columnList.begin()) {
			flagsEntry.add(IndexFlagsEntry::FLAGS_TYPE_COMPOSITE_TOP, flags);
		}
	}
}

void SQLExprs::IndexSelector::completeIndex() {
	if (!cleared_) {
		std::sort(specList_.begin(), specList_.end());
	}

	columnSpecIdList_.clear();
	for (SpecList::const_iterator specIt = specList_.begin();
			specIt != specList_.end(); ++specIt) {
		const IndexSpecId specId =
				static_cast<IndexSpecId>(specIt - specList_.begin()) + 1;

		for (IndexSpec::ColumnsIterator it = specIt->columnsBegin();
				it != specIt->columnsEnd(); ++it) {
			columnSpecIdList_.push_back(ColumnSpecId(*it, specId));
		}
	}
	std::sort(columnSpecIdList_.begin(), columnSpecIdList_.end());

	completed_ = true;
}

void SQLExprs::IndexSelector::setMultiAndConditionEnabled(
		bool enabled) {
	multiAndConditionEnabled_ = enabled;
}

void SQLExprs::IndexSelector::setBulkGrouping(bool enabled) {
	bulkGrouping_ = enabled;
}

bool SQLExprs::IndexSelector::matchIndexList(
		const int32_t *indexList, size_t size) const {
	if (indexList_.size() != size) {
		return false;
	}

	for (size_t i = 0; i < size; i++) {
		const IndexFlagsEntry &flagsEntry = indexList_[i];
		const IndexFlags flags =
				flagsEntry.get(IndexFlagsEntry::FLAGS_TYPE_SINGLE) |
				flagsEntry.get(IndexFlagsEntry::FLAGS_TYPE_COMPOSITE_TOP);

		if (flags != static_cast<IndexFlags>(indexList[i])) {
			return false;
		}
	}

	return true;
}

bool SQLExprs::IndexSelector::updateIndex(size_t condIndex) {
	if (!indexAvailable_) {
		return false;
	}

	indexAvailable_ &= findIndex(condIndex);
	return indexAvailable_;
}

bool SQLExprs::IndexSelector::findIndex(size_t condIndex) const {
	return getAvailableIndex(condList_[condIndex], true, NULL, NULL, NULL);
}

SQLType::IndexType SQLExprs::IndexSelector::getIndexType(
		const Condition &cond) const {
	SQLType::IndexType indexType;
	if (!getAvailableIndex(cond, true, &indexType, NULL, NULL)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return indexType;
}

SQLExprs::IndexSelector::IndexSpecId
SQLExprs::IndexSelector::getMaxSpecId() const {
	if (!completed_) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return specList_.size();
}

size_t SQLExprs::IndexSelector::getIndexColumnList(
		IndexSpecId specId, const uint32_t *&list) const {
	if (!completed_ || specId <= 0 || specId > specList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const IndexSpec &spec = specList_[static_cast<size_t>(specId - 1)];
	assert(spec.getColumnCount() > 0);

	list = &(*spec.columnsBegin());
	return spec.getColumnCount();
}

void SQLExprs::IndexSelector::select(const Expression &expr) {
	selectDetail(NULL, &expr);
}

void SQLExprs::IndexSelector::selectDetail(
		const Expression *proj, const Expression *pred) {
	condList_.clear();
	placeholderAffected_ = false;
	do {
		if (proj != NULL) {
			AggregationMatcher matcher(stdAlloc_, *this, localValuesHolder_);
			if (matcher.match(*proj, pred, condList_)) {
				break;
			}
		}

		if (pred != NULL) {
			selectSub(*pred, false, false);
			break;
		}

		condList_.push_back(Condition());
	}
	while (false);

	for (ConditionList::iterator it = condList_.begin();
			it != condList_.end(); ++it) {
		if (it->specId_ > 0) {
			it->spec_ = specList_[static_cast<size_t>(it->specId_ - 1)];
		}
	}

	if (bulkGrouping_) {
		BulkRewriter rewriter;
		rewriter.rewrite(condList_);
	}
}

bool SQLExprs::IndexSelector::isSelected() const {
	if (condList_.empty()) {
		return false;
	}
	else if (condList_.size() > 1) {
		return true;
	}

	const Condition &cond = condList_.front();
	return (!cond.isEmpty() && cond.opType_ != SQLType::EXPR_CONSTANT);
}

bool SQLExprs::IndexSelector::isComplex() const {
	return getOrConditionCount(
			condList_.begin(), condList_.end()) != condList_.size();
}

const SQLExprs::IndexSelector::ConditionList&
SQLExprs::IndexSelector::getConditionList() const {
	return condList_;
}

SQLExprs::IndexSelector::ConditionList&
SQLExprs::IndexSelector::getConditionList() {
	return condList_;
}

void SQLExprs::IndexSelector::getIndexColumnList(
		const Condition &cond, util::Vector<uint32_t> &columnList) const {
	if (cond.specId_ > 0) {
		if (cond.specId_ > specList_.size()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		const size_t pos = static_cast<size_t>(cond.specId_ - 1);
		const IndexSpec &spec = specList_[pos];
		for (IndexSpec::ColumnsIterator it = spec.columnsBegin();
				it != spec.columnsEnd(); ++it) {
			columnList.push_back(*it);
		}
	}
	else {
		if (cond.column_ != Condition::EMPTY_IN_COLUMN) {
			columnList.push_back(cond.column_);
		}
	}
}

bool SQLExprs::IndexSelector::bindCondition(
		Condition &cond,
		const TupleValue &value1, const TupleValue &value2,
		SQLValues::ValueSetHolder &valuesHolder) const {
	valuesHolder.reset();

	if (cond.isEmpty() || cond.opType_ == SQLType::EXPR_CONSTANT) {
		return false;
	}

	const Condition emptyCond;
	Condition subCondList[2];
	const size_t subCondCount =
			(cond.opType_ == SQLType::EXPR_BETWEEN ? 2 : 1);

	for (size_t i = 0; i < subCondCount; i++) {
		Condition &subCond = subCondList[i];
		subCond = cond;

		subCond.valuePair_ = emptyCond.valuePair_;
		subCond.inclusivePair_ = emptyCond.inclusivePair_;
		subCond.inColumnPair_ = emptyCond.inColumnPair_;

		const uint32_t inColumn = getPairElement(i, cond.inColumnPair_);
		if (inColumn == Condition::EMPTY_IN_COLUMN) {
			subCond.opType_ = cond.opType_;
			getPairElement(i, subCond.valuePair_) =
					getPairElement(i, cond.valuePair_);
			getPairElement(i, subCond.inclusivePair_) =
					getPairElement(i, cond.inclusivePair_);
			continue;
		}

		if (cond.opType_ != SQLType::EXPR_BETWEEN) {
			subCond.opType_ = SQLType::OP_EQ;
		}
		else if (getPairElement(i, cond.inclusivePair_)) {
			subCond.opType_ = (i == 0 ? SQLType::OP_GE : SQLType::OP_LE);
		}
		else {
			subCond.opType_ = (i == 0 ? SQLType::OP_GT : SQLType::OP_LT);
		}

		const TupleValue &value = (i == 0 ? value1 : value2);
		const bool rangePreferred = false;
		if (!applyValue(
				subCond, &value, Condition::EMPTY_IN_COLUMN, valuesHolder,
				rangePreferred)) {
			cond = Condition();
			return false;
		}
	}

	if (subCondCount > 1) {
		cond = makeNarrower(subCondList[0], subCondList[1]);
	}
	else {
		cond = subCondList[0];
	}
	assert(cond.isBinded());

	return getAvailableIndex(cond, true, NULL, NULL, NULL);
}

bool SQLExprs::IndexSelector::isPlaceholderAffected() const {
	return placeholderAffected_;
}

size_t SQLExprs::IndexSelector::getOrConditionCount(
		ConditionList::const_iterator beginIt,
		ConditionList::const_iterator endIt) {
	size_t count = 0;
	for (ConditionList::const_iterator it = beginIt; it != endIt;) {
		it += nextOrConditionDistance(it, endIt);
		count++;
	}
	return count;
}

size_t SQLExprs::IndexSelector::nextOrConditionDistance(
		ConditionList::const_iterator it,
		ConditionList::const_iterator endIt) {
	assert(it != endIt);
	assert(it->isAndTop());

	const size_t maxCount = static_cast<size_t>(endIt - it);
	assert(it->andCount_ <= maxCount);

	return std::min(it->andCount_, maxCount);
}

size_t SQLExprs::IndexSelector::nextCompositeConditionDistance(
		ConditionList::const_iterator beginIt,
		ConditionList::const_iterator endIt) {
	return nextCompositeConditionDistance(beginIt, endIt, ConditionAccessor());
}

template<typename It, typename Accessor>
size_t SQLExprs::IndexSelector::nextCompositeConditionDistance(
		It beginIt, It endIt, const Accessor &accessor) {

	It it = beginIt;
	for (; it != endIt; ++it) {
		const Condition &cond = accessor(it);
		if (cond.andCount_ <= 1) {
			if (it == beginIt) {
				++it;
			}
			break;
		}

		if (it == beginIt) {
			continue;
		}

		const Condition &prevCond = accessor(it - 1);
		if (cond.andOrdinal_ < prevCond.andOrdinal_) {
			break;
		}
		if (cond.compositeAndOrdinal_ != prevCond.compositeAndOrdinal_) {
			break;
		}
	}
	return static_cast<size_t>(it - beginIt);
}

size_t SQLExprs::IndexSelector::nextBulkConditionDistance(
		ConditionList::const_iterator beginIt,
		ConditionList::const_iterator endIt) {
	ConditionList::const_iterator it = beginIt;
	if (it != endIt) {
		assert(it->bulkOrCount_ > 0);
		assert(it->isBulkTop());
		for (size_t i = it->bulkOrCount_; i > 0; --i) {
			assert(it != endIt);
			const size_t size = nextOrConditionDistance(it, endIt);
			assert(size > 0 && size <= static_cast<size_t>(endIt - it));
			it += size;
		}
	}
	return static_cast<size_t>(it - beginIt);
}

void SQLExprs::IndexSelector::selectSub(
		const Expression &expr, bool negative, bool inAndCond) {
	const ExprType exprType = expr.getCode().getType();

	if (exprType == SQLType::EXPR_CONSTANT) {
		const TupleValue &value = expr.getCode().getValue();

		bool nullFound;
		const bool boolValue = isTrueValue(value, negative, nullFound);

		condList_.push_back((nullFound ?
				Condition::createNull() : Condition::createBool(boolValue)));
	}
	else if (exprType ==
			ExprTypeUtils::getLogicalOp(SQLType::EXPR_AND, negative)) {
		bool complexLeading = false;
		selectSubAnd(expr, NULL, complexLeading, negative);
	}
	else if (exprType ==
			ExprTypeUtils::getLogicalOp(SQLType::EXPR_OR, negative)) {
		const size_t orgSize = condList_.size();
		for (const Expression *sub = expr.findChild();
				sub != NULL; sub = sub->findNext()) {
			selectSub(*sub, negative, false);
		}
		reduceConditionList(orgSize);
	}
	else if (exprType == SQLType::OP_NOT) {
		selectSub(expr.child(), !negative, inAndCond);
	}
	else {
		condList_.push_back(makeValueCondition(
				expr, negative, placeholderAffected_, localValuesHolder_));

		if (!inAndCond) {
			Range range;
			range.second(condList_.size());
			range.first(range.second() - 1);

			const bool atAndTop = true;
			assignAndOrdinals(range, atAndTop);
		}
	}
}

void SQLExprs::IndexSelector::selectSubAnd(
		const Expression &expr, Range *simpleRangeRef, bool &complexLeading,
		bool negative) {
	const ExprType logicalOp =
			ExprTypeUtils::getLogicalOp(SQLType::EXPR_AND, negative);
	assert(expr.getCode().getType() == logicalOp);

	Range localRange;
	Range &simpleRange =
			(simpleRangeRef == NULL ? localRange : *simpleRangeRef);
	const bool atAndTop = (simpleRangeRef == NULL);

	if (atAndTop) {
		const size_t size = condList_.size();
		simpleRange.first(size);
		simpleRange.second(size);
	}

	for (const Expression *sub = expr.findChild();
			sub != NULL; sub = sub->findNext()) {
		if (sub->getCode().getType() == logicalOp) {
			selectSubAnd(*sub, &simpleRange, complexLeading, negative);
		}
		else {
			const size_t orgSize = condList_.size();
			selectSub(*sub, negative, true);

			if (getOrConditionCount(orgSize, condList_.size()) == 1) {
				narrowSimpleCondition(simpleRange, orgSize, false);
			}
			else {
				if (simpleRange.second() == orgSize &&
						(simpleRange.first() == simpleRange.second() ||
						condList_[simpleRange.first()].isEmpty())) {
					complexLeading = true;
				}
				narrowComplexCondition(simpleRange.second(), orgSize, true);
			}
		}
	}

	if (atAndTop) {
		assignAndOrdinals(simpleRange, atAndTop);
		narrowComplexCondition(
				simpleRange.first(), simpleRange.second(), !complexLeading);
	}
}

void SQLExprs::IndexSelector::narrowSimpleCondition(
		Range &simpleRange, size_t offset, bool atAndTop) {
	assert(getOrConditionCount(offset, condList_.size()) == 1);
	assert(simpleRange.second() <= offset);

	if (simpleRange.first() == simpleRange.second()) {
		const size_t endPos = simpleRange.second();
		assert(endPos == condList_.size() || condList_[endPos].isAndTop());

		const size_t count = moveTailConditionList(endPos, offset);
		simpleRange.second(endPos + count);
		return;
	}

	assert(getOrConditionCount(
			simpleRange.first(), simpleRange.second()) == 1);

	bool trueFound = false;
	bool nullFound = false;
	bool emptyFound = false;
	bool falseFound = false;
	for (ConditionList::iterator it2 = condList_.begin() + offset;
			it2 != condList_.end(); ++it2) {
		ConditionList::iterator endIt1 =
				condList_.begin() + simpleRange.second();
		for (ConditionList::iterator it1 =
				condList_.begin() + simpleRange.first();
				it1 != endIt1; ++it1) {
			Condition cond;
			if (tryMakeNarrower(*it1, *it2, cond)) {
				if (cond.isStatic()) {
					if (cond.equals(true)) {
						trueFound = true;
					}
					else if (cond.isEmpty()) {
						emptyFound = true;
					}
					else if (cond.isNull()) {
						nullFound = true;
					}
					else if (cond.equals(false)) {
						falseFound = true;
					}
					cond = Condition();
				}
				*it1 = cond;
				*it2 = Condition();
				break;
			}
		}
	}
	const bool negative = (falseFound || nullFound);

	if (negative) {
		condList_.erase(condList_.begin() + offset, condList_.end());
	}
	else {
		eraseEmptyConditionList(offset, condList_.size());
		moveTailConditionList(simpleRange.second(), offset);

		const size_t count = condList_.size() - offset;
		simpleRange.second(simpleRange.second() + count);
	}

	if (negative) {
		condList_.erase(
				condList_.begin() + simpleRange.first(),
				condList_.begin() + simpleRange.second());
		simpleRange.second(simpleRange.first());
	}
	else {
		const size_t count = eraseEmptyConditionList(
				simpleRange.first(), simpleRange.second());
		simpleRange.second(simpleRange.second() - count);
	}

	const size_t andCount = simpleRange.second() - simpleRange.first();
	if (andCount == 0) {
		Condition cond;
		if (falseFound) {
			cond = Condition::createBool(false);
		}
		else if (nullFound) {
			cond = Condition::createNull();
		}
		else if (!emptyFound && trueFound) {
			cond = Condition::createBool(true);
		}
		ConditionList::iterator it = condList_.begin() + simpleRange.first();
		condList_.insert(it, cond);
		simpleRange.second(simpleRange.second() + 1);
	}
	else {
		assignAndOrdinals(simpleRange, atAndTop);
	}
}

void SQLExprs::IndexSelector::narrowComplexCondition(
		size_t firstOffset, size_t secondOffset, bool firstLeading) {
	if (firstOffset == secondOffset || secondOffset >= condList_.size()) {
		return;
	}

	bool firstNarrow;
	do {
		const size_t firstOrCount =
				getOrConditionCount(firstOffset, secondOffset);
		if (firstOrCount == 1) {
			const Condition &cond = condList_[firstOffset];
			if (cond.isEmpty() || cond.opType_ == SQLType::EXPR_CONSTANT) {
				firstNarrow = (cond.equals(false) || cond.isNull());
				break;
			}
		}

		if (!complexReordering_) {
			firstNarrow = firstLeading;
			break;
		}

		bool eqList[] = { true, true };
		for (size_t i = 0; i < 2; i++) {
			const ConditionList::iterator &begin =
					condList_.begin() + (i ==0 ? firstOffset : secondOffset);
			const ConditionList::iterator &end =
					(i ==0 ? condList_.begin() + secondOffset : condList_.end());

			for (ConditionList::iterator it = begin; it != end; ++it) {
				if (it->opType_ != SQLType::OP_EQ) {
					eqList[i] = false;
					break;
				}
			}
		}

		if (!eqList[0] != !eqList[1]) {
			firstNarrow = eqList[0];
		}
		else {
			const size_t secondOrCount =
					getOrConditionCount(secondOffset, condList_.size());
			firstNarrow = (firstOrCount <= secondOrCount);
		}
	}
	while (false);

	if (firstNarrow) {
		condList_.erase(condList_.begin() + secondOffset, condList_.end());
	}
	else {
		condList_.erase(
				condList_.begin() + firstOffset,
				condList_.begin() + secondOffset);
	}
}

void SQLExprs::IndexSelector::assignAndOrdinals(
		Range &totalRange, bool atAndTop) {
	if (totalRange.first() < totalRange.second()) {
		const Condition &cond = condList_[totalRange.first()];
		if (cond.isEmpty() || cond.opType_ == SQLType::EXPR_CONSTANT) {
			assert(totalRange.second() - totalRange.first() == 1);
			return;
		}
	}

	size_t compAndOrdinal = 0;
	const size_t compCheckStartPos =
			(atAndTop ? totalRange.first() : totalRange.second());

	bool first = true;
	for (size_t pos = compCheckStartPos; pos < totalRange.second();) {
		const size_t acceptedCondCount = pos - totalRange.first();

		size_t count;
		if (atAndTop && !first && !multiAndConditionEnabled_) {
			count = 0;
		}
		else {
			count = matchAndConditions(totalRange, acceptedCondCount);
		}
		first = false;

		if (count == 0) {
			condList_.erase(
					condList_.begin() + pos,
					condList_.begin() + totalRange.second());
			totalRange.second(pos);

			if (totalRange.first() == totalRange.second()) {
				condList_.insert(
						condList_.begin() + totalRange.first(), Condition());
			}
			break;
		}

		const size_t nextPos = pos + count;
		for (; pos < nextPos; ++pos) {
			Condition &cond = condList_[pos];
			cond.andOrdinal_ = pos - totalRange.first();
			cond.compositeAndOrdinal_ = compAndOrdinal;
		}

		++compAndOrdinal;
	}

	const size_t andCount = totalRange.second() - totalRange.first();
	const size_t compAndCount = compAndOrdinal;
	for (size_t pos = totalRange.first(); pos < totalRange.second(); pos++) {
		Condition &cond = condList_[pos];
		cond.andCount_ = andCount;
		cond.compositeAndCount_ = (atAndTop ? compAndCount : andCount);
	}
}

size_t SQLExprs::IndexSelector::matchAndConditions(
		Range &totalRange, size_t acceptedCondCount) {
	ConditionList::iterator condBegin = condList_.begin() + totalRange.first();
	ConditionList::iterator condEnd = condList_.begin() + totalRange.second();

	size_t condOrdinal;
	IndexSpecId specId;
	if (!selectClosestIndex(
			condBegin, condEnd, acceptedCondCount, condOrdinal, specId)) {
		return 0;
	}

	if (specId <= 0) {
		assert(condOrdinal >= acceptedCondCount);
		moveTailConditionToFront(
				condBegin + acceptedCondCount,
				condBegin + condOrdinal);
		return 1;
	}

	ConditionOrdinalList &ordinalList = condOrdinalList_;
	ordinalList.clear();
	ordinalList.push_back(condOrdinal);

	const IndexSpec &spec = specList_[static_cast<size_t>(specId - 1)];
	for (IndexSpec::ColumnsIterator it = spec.columnsBegin();
			it != spec.columnsEnd(); ++it) {
		size_t nextCondOrdinal;
		if (!selectClosestIndexColumn(
				condBegin, condEnd, acceptedCondCount, condOrdinal, *it,
				specId, nextCondOrdinal)) {
			break;
		}
		ordinalList.push_back(nextCondOrdinal);
		condOrdinal = nextCondOrdinal;
	}

	return arrangeMatchedAndConditions(
			totalRange, acceptedCondCount, ordinalList, specId);
}

size_t SQLExprs::IndexSelector::arrangeMatchedAndConditions(
		Range &totalRange, size_t acceptedCondCount,
		ConditionOrdinalList &ordinalList, IndexSpecId specId) {
	{
		std::sort(ordinalList.begin(), ordinalList.end());
		ConditionOrdinalList::iterator uniqueTail =
				std::unique(ordinalList.begin(), ordinalList.end());
		ordinalList.erase(uniqueTail, ordinalList.end());
	}
	const size_t ordinalCount = ordinalList.size();

	const size_t startPos = totalRange.first();
	const size_t acceptingPos = startPos + acceptedCondCount;
	{
		ConditionList::iterator destIt = condList_.begin() + acceptingPos;
		for (ConditionOrdinalList::iterator ordIt = ordinalList.begin();
				ordIt != ordinalList.end(); ++ordIt) {
			const size_t ordinal = static_cast<size_t>(*ordIt);
			if (ordinal < acceptedCondCount) {
				continue;
			}

			const size_t ordPos = startPos + ordinal;
			assert(ordPos < totalRange.second());

			assert(static_cast<size_t>(destIt - condList_.begin()) <
					totalRange.second());
			moveTailConditionToFront(destIt, condList_.begin() + ordPos);
			++destIt;
		}
	}

	while (!ordinalList.empty()) {
		const size_t ordinal = static_cast<size_t>(ordinalList.back());
		if (ordinal < acceptedCondCount) {
			Condition cond = condList_[startPos + ordinal];
			condList_.insert(condList_.begin() + acceptingPos, cond);
			totalRange.second(totalRange.second() + 1);
		}
		ordinalList.pop_back();
	}

	{
		ConditionList::iterator it = condList_.begin() + acceptingPos;
		ConditionList::iterator endIt = it + ordinalCount;
		for (; it != endIt; ++it) {
			it->specId_ = specId;
		}
	}

	return ordinalCount;
}

void SQLExprs::IndexSelector::moveTailConditionToFront(
		ConditionList::iterator beginIt, ConditionList::iterator tailIt) {
	if (beginIt == tailIt) {
		return;
	}

	assert(tailIt - beginIt > 0);
	const Condition cond = *tailIt;
	for (ConditionList::iterator it = tailIt; it != beginIt; --it) {
		*it = *(it - 1);
	}
	*beginIt = cond;
}

SQLExprs::IndexSelector::Condition
SQLExprs::IndexSelector::makeNarrower(
		const Condition &cond1, const Condition &cond2) {
	Condition retCond;
	tryMakeNarrower(cond1, cond2, retCond);
	return retCond;
}

bool SQLExprs::IndexSelector::tryMakeNarrower(
		const Condition &cond1, const Condition &cond2, Condition &retCond) {
	int32_t levelList[] = { 0, 0 };
	for (size_t i = 0; i < 2; i++) {
		const Condition &cond = (i ==0 ? cond1 : cond2);
		if (cond.equals(true)) {
			levelList[i] = -2;
		}
		else if (cond.isEmpty()) {
			levelList[i] = -1;
		}
		else if (cond.isNull()) {
			levelList[i] = 1;
		}
		else if (cond.equals(false)) {
			levelList[i] = 2;
		}
	}

	if (levelList[0] != 0 || levelList[1] != 0) {
		if (levelList[0] >= levelList[1]) {
			retCond = cond1;
		}
		else {
			retCond = cond2;
		}
		return true;
	}
	else if (cond1.column_ != cond2.column_) {
		retCond = cond1;
		return false;
	}

	retCond = makeRangeNarrower(
			toRangeCondition(cond1), toRangeCondition(cond2));
	return true;
}

SQLExprs::IndexSelector::Condition
SQLExprs::IndexSelector::makeRangeNarrower(
		const Condition &cond1, const Condition &cond2) {
	assert(cond1.column_ == cond2.column_);

	assert(cond1.opType_ == SQLType::EXPR_BETWEEN);
	assert(cond2.opType_ == SQLType::EXPR_BETWEEN);

	const Condition emptyCond;
	Condition cond = cond1;
	cond.opType_ = SQLType::EXPR_BETWEEN;

	cond.valuePair_ = emptyCond.valuePair_;
	cond.inclusivePair_ = emptyCond.inclusivePair_;
	cond.inColumnPair_ = emptyCond.inColumnPair_;

	for (size_t i = 0; i < 2; i++) {
		TupleValue &value = getPairElement(i, cond.valuePair_);
		const TupleValue &value1 = getPairElement(i, cond1.valuePair_);
		const TupleValue &value2 = getPairElement(i, cond2.valuePair_);

		bool &inclusive = getPairElement(i, cond.inclusivePair_);
		const bool inclusive1 = getPairElement(i, cond1.inclusivePair_);
		const bool inclusive2 = getPairElement(i, cond2.inclusivePair_);

		uint32_t &inColumn = getPairElement(i, cond.inColumnPair_);
		const uint32_t inColumn1 = getPairElement(i, cond1.inColumnPair_);
		const uint32_t inColumn2 = getPairElement(i, cond2.inColumnPair_);

		if (inColumn1 != Condition::EMPTY_IN_COLUMN) {
			inColumn = inColumn1;
			inclusive = inclusive1;
		}
		else if (inColumn2 != Condition::EMPTY_IN_COLUMN) {
			inColumn = inColumn2;
			inclusive = inclusive2;
		}
		else if (SQLValues::ValueUtils::isNull(value1)) {
			if (SQLValues::ValueUtils::isNull(value2)) {
				value = TupleValue();
			}
			else {
				value = value2;
				inclusive = inclusive2;
			}
		}
		else {
			if (SQLValues::ValueUtils::isNull(value2)) {
				value = value1;
				inclusive = inclusive1;
			}
			else {
				const int32_t comp =
						compareValue(value1, value2) * (i == 0 ? 1 : -1);

				if (comp < 0) {
					value = value2;
					inclusive = inclusive2;
				}
				else if (comp > 0) {
					value = value1;
					inclusive = inclusive1;
				}
				else {
					value = value1;
					inclusive = inclusive1 && inclusive2;
				}
			}
		}
	}

	{
		const TupleValue &startValue = cond.valuePair_.first;
		const TupleValue &endValue = cond.valuePair_.second;

		if (!SQLValues::ValueUtils::isNull(startValue) &&
				!SQLValues::ValueUtils::isNull(endValue)) {

			const int32_t comp = compareValue(startValue, endValue);

			if (comp > 0) {
				return Condition::createBool(false);
			}
			else if (comp == 0) {
				if (!cond.inclusivePair_.first ||
						!cond.inclusivePair_.second) {
					return Condition::createBool(false);
				}
				else {
					cond.opType_ = SQLType::OP_EQ;
					cond.valuePair_.second = TupleValue();
					cond.inclusivePair_ = std::pair<bool, bool>(true, true);
					return cond;
				}
			}
		}
		else if (cond.inColumnPair_.first != Condition::EMPTY_IN_COLUMN &&
				cond.inColumnPair_.first == cond.inColumnPair_.second) {
			if (cond.inclusivePair_.first && cond.inclusivePair_.second) {
				cond.opType_ = SQLType::OP_EQ;
				cond.inColumnPair_.second = Condition::EMPTY_IN_COLUMN;
				cond.inclusivePair_ = std::pair<bool, bool>(true, true);
				return cond;
			}
		}
	}

	return cond;
}

SQLExprs::IndexSelector::Condition
SQLExprs::IndexSelector::toRangeCondition(const Condition &cond) {
	if (cond.opType_ == SQLType::OP_EQ) {
		Condition destCond = cond;
		destCond.opType_ = SQLType::EXPR_BETWEEN;
		destCond.valuePair_.second = destCond.valuePair_.first;
		destCond.inColumnPair_.second = destCond.inColumnPair_.first;
		destCond.inclusivePair_ = std::make_pair(true, true);
		return destCond;
	}
	else {
		return cond;
	}
}

size_t SQLExprs::IndexSelector::moveTailConditionList(
		size_t destOffset, size_t srcOffset) {
	assert(destOffset <= condList_.size());
	assert(srcOffset <= condList_.size());

	const size_t count = condList_.size() - srcOffset;

	if (destOffset >= srcOffset) {
		assert(destOffset == srcOffset);
		return count;
	}

	condList_.insert(condList_.begin() + destOffset, count, Condition());

	ConditionList::iterator destBegin = condList_.begin() + destOffset;
	ConditionList::iterator destIt = destBegin + count;
	ConditionList::iterator srcIt = condList_.end();

	while (destIt != destBegin) {
		*(--destIt) = *(--srcIt);
	}

	condList_.erase(srcIt, condList_.end());
	return count;
}

size_t SQLExprs::IndexSelector::eraseEmptyConditionList(
		size_t beginOffset, size_t endOffset) {
	assert(beginOffset <= condList_.size());
	assert(endOffset <= condList_.size());

	size_t eraseCount = 0;
	if (beginOffset >= endOffset) {
		return eraseCount;
	}

	ConditionList::iterator srcIt = condList_.begin() + beginOffset;
	ConditionList::iterator destIt = srcIt;
	ConditionList::iterator endIt = condList_.begin() + endOffset;

	for (; srcIt != endIt; ++srcIt) {
		if (!srcIt->isEmpty()) {
			if (destIt != srcIt) {
				*destIt = *srcIt;
			}
			++destIt;
		}
	}

	eraseCount += (endIt - destIt);
	condList_.erase(destIt, endIt);
	return eraseCount;
}

void SQLExprs::IndexSelector::reduceConditionList(size_t offset) {
	bool emptyFound = false;
	bool nullFound = false;
	bool falseFound = false;

	for (ConditionList::iterator it =
			condList_.begin() + std::min(offset, condList_.size());
			it != condList_.end();) {
		assert(it->isAndTop());

		if (it->equals(true)) {
			const Condition cond = *it;
			condList_.resize(offset);
			condList_.push_back(cond);
			return;
		}

		do {
			if (it->isEmpty()) {
				emptyFound = true;
			}
			else if (it->isNull()) {
				nullFound = true;
			}
			else if (it->equals(false)) {
				falseFound = true;
			}
			else {
				 it += nextOrConditionDistance(it, condList_.end());
				 break;
			}
			assert(nextOrConditionDistance(it, condList_.end()) == 1);
			it = condList_.erase(it);
		}
		while (false);
	}

	if (emptyFound) {
		condList_.resize(offset);
		condList_.push_back(Condition());
		return;
	}

	if (condList_.size() <= offset) {
		if (nullFound) {
			condList_.push_back(Condition::createNull());
		}
		else if (falseFound) {
			condList_.push_back(Condition::createBool(false));
		}
	}
}

size_t SQLExprs::IndexSelector::getOrConditionCount(
		size_t beginOffset, size_t endOffset) const {
	assert(beginOffset <= condList_.size());
	assert(endOffset <= condList_.size());
	return getOrConditionCount(
			condList_.begin() + beginOffset, condList_.begin() + endOffset);
}

SQLExprs::IndexSelector::Condition
SQLExprs::IndexSelector::makeValueCondition(
		const Expression &expr, bool negative, bool &placeholderAffected,
		SQLValues::ValueSetHolder &valuesHolder,
		bool topColumnOnly, bool treeIndexExtended) const {
	ExprType opType = expr.getCode().getType();

	const ExprType negativeOpType = ExprTypeUtils::negateCompOp(opType);
	if (opType == negativeOpType) {
		return Condition();
	}

	if (negative) {
		opType = negativeOpType;
	}

	const Expression *columnExpr = &expr.child();
	const Expression *valueExpr = &columnExpr->next();
	uint32_t inColumn = Condition::EMPTY_IN_COLUMN;

	placeholderAffected |= ExprRewriter::isConstWithPlaceholder(
			*exprFactory_, *columnExpr).first;
	placeholderAffected |= ExprRewriter::isConstWithPlaceholder(
			*exprFactory_, *valueExpr).first;

	const TupleValue *value = findConst(*valueExpr);
	if (value == NULL && !findColumn(*valueExpr, inColumn, false)) {
		std::swap(columnExpr, valueExpr);
		value = findConst(*valueExpr);
		if (value == NULL && !findColumn(*valueExpr, inColumn, false)) {
			return Condition();
		}

		opType = ExprTypeUtils::swapCompOp(opType);
	}

	Condition cond;
	if (!findColumn(*columnExpr, cond.column_, true)) {
		return Condition();
	}
	cond.opType_ = opType;

	const bool rangePreferred = treeIndexExtended;
	if (!applyValue(cond, value, inColumn, valuesHolder, rangePreferred)) {
		return Condition();
	}

	if (cond.opType_ != SQLType::EXPR_CONSTANT) {
		IndexSpecId specId;
		if (!getAvailableIndex(
				cond, false, NULL, &specId, NULL, topColumnOnly,
				treeIndexExtended)) {
			return Condition();
		}
		cond.specId_ = specId;
	}

	return cond;
}

bool SQLExprs::IndexSelector::applyValue(
		Condition &cond, const TupleValue *value, uint32_t inColumn,
		SQLValues::ValueSetHolder &valuesHolder, bool rangePreferred) const {
	if (value == NULL) {
		assert(inColumn != Condition::EMPTY_IN_COLUMN);
		cond.inColumnPair_.first = inColumn;
	}
	else {
		if (SQLValues::ValueUtils::isNull(*value)) {
			cond = Condition::createNull();
			return true;
		}

		cond.valuePair_.first = *value;

		const TupleColumnType columnType =
				SQLValues::TypeUtils::toNonNullable(tupleInfo_[cond.column_]);

		{
			const TupleColumnType valueType = value->getType();
			const bool implicit = true;
			const bool anyAsNull = true;
			if (SQLValues::TypeUtils::isNull(SQLValues::TypeUtils::findConversionType(
					valueType, columnType, implicit, anyAsNull))) {
				cond = Condition();
				return false;
			}
		}

		switch (columnType) {
		case TupleTypes::TYPE_BYTE:
			applyIntegralBounds<TupleTypes::TYPE_BYTE>(cond, rangePreferred);
			break;
		case TupleTypes::TYPE_SHORT:
			applyIntegralBounds<TupleTypes::TYPE_SHORT>(cond, rangePreferred);
			break;
		case TupleTypes::TYPE_INTEGER:
			applyIntegralBounds<TupleTypes::TYPE_INTEGER>(cond, rangePreferred);
			break;
		case TupleTypes::TYPE_LONG:
			applyIntegralBounds<TupleTypes::TYPE_LONG>(cond, rangePreferred);
			break;
		case TupleTypes::TYPE_FLOAT:
			applyFloatingBounds<TupleTypes::TYPE_FLOAT>(cond);
			break;
		case TupleTypes::TYPE_DOUBLE:
			applyFloatingBounds<TupleTypes::TYPE_DOUBLE>(cond);
			break;
		case TupleTypes::TYPE_BOOL:
			applyBoolBounds(cond, rangePreferred);
			break;
		default:
			if (SQLValues::TypeUtils::isTimestampFamily(columnType)) {
				applyTimestampBounds(cond, columnType, valuesHolder);
			}
			break;
		}

		if (cond.opType_ == SQLType::EXPR_CONSTANT) {
			return true;
		}

		if (cond.isEmpty() || cond.valuePair_.first.getType() != columnType) {
			cond = Condition();
			return false;
		}
	}

	const ExprType opType = cond.opType_;

	if (opType == SQLType::OP_LT || opType == SQLType::OP_LE) {
		if (value == NULL) {
			std::swap(cond.inColumnPair_.first, cond.inColumnPair_.second);
		}
		else {
			cond.valuePair_.second = cond.valuePair_.first;
			cond.valuePair_.first = TupleValue();
		}
	}

	if (opType == SQLType::OP_LE) {
		cond.inclusivePair_.second = true;
	}
	else if (opType == SQLType::OP_GE || opType == SQLType::OP_EQ) {
		cond.inclusivePair_.first = true;
	}

	if (isRangeConditionType(opType)) {
		cond.opType_ = SQLType::EXPR_BETWEEN;
	}

	return true;
}

bool SQLExprs::IndexSelector::isRangeConditionType(ExprType type) {
	switch (type) {
	case SQLType::OP_LT:
	case SQLType::OP_LE:
	case SQLType::OP_GT:
	case SQLType::OP_GE:
		return true;
	default:
		return false;
	}
}

void SQLExprs::IndexSelector::applyBoolBounds(
		Condition &cond, bool rangePreferred) {
	const int64_t min = 0;
	const int64_t max = 1;
	int64_t value;
	if (applyIntegralBounds(cond, min, max, rangePreferred, value)) {
		cond.valuePair_.first = SQLValues::ValueUtils::toAnyByBool(!!value);
	}
}

template<TupleColumnType T>
void SQLExprs::IndexSelector::applyIntegralBounds(
		Condition &cond, bool rangePreferred) {
	typedef typename SQLValues::TypeUtils::Traits<T>::ValueType ValueType;

	const int64_t min = std::numeric_limits<ValueType>::min();
	const int64_t max = std::numeric_limits<ValueType>::max();
	int64_t value;
	if (applyIntegralBounds(cond, min, max, rangePreferred, value)) {
		cond.valuePair_.first = TupleValue(static_cast<ValueType>(value));
	}
}

bool SQLExprs::IndexSelector::applyIntegralBounds(
		Condition &cond, int64_t min, int64_t max, bool rangePreferred,
		int64_t &value) {
	const TupleValue &condValue = cond.valuePair_.first;

	const bool strict = false;
	if (!SQLValues::ValueUtils::toLongDetail(condValue, value, strict)) {
		cond = Condition();
		return false;
	}

	ExprType &opType = cond.opType_;

	if (SQLValues::TypeUtils::isFloating(condValue.getType())) {
		const int32_t comp = compareValue(
				SQLValues::ValueUtils::toAnyByNumeric(value), condValue);
		if (comp != 0) {
			switch (opType) {
			case SQLType::OP_LT:
				opType = SQLType::OP_LE;
				break;
			case SQLType::OP_GT:
				opType = SQLType::OP_GE;
				break;
			default:
				break;
			}
			double doubleValue;
			if (SQLValues::ValueUtils::toDoubleDetail(
					condValue, doubleValue, strict) &&
					util::isNaN(doubleValue)) {
				value = max;
			}
		}
	}

	if (max - min == 1 && opType == SQLType::OP_NE) {
		if (value == min) {
			value = max;
		}
		else {
			value = min;
		}
		opType = SQLType::OP_EQ;
	}

	switch (opType) {
	case SQLType::OP_LT:
		if (value > min) {
			value--;
			opType = SQLType::OP_LE;
		}
		break;
	case SQLType::OP_GT:
		if (value < max) {
			value++;
			opType = SQLType::OP_GE;
		}
		break;
	default:
		break;
	}

	if (value == min) {
		switch (opType) {
		case SQLType::OP_LT:
			cond = Condition::createBool(false);
			break;
		case SQLType::OP_LE:
			if (!rangePreferred) {
				cond.opType_ = SQLType::OP_EQ;
			}
			break;
		case SQLType::OP_GE:
			cond = Condition::createBool(true);
			break;
		default:
			break;
		}
	}
	else if (value == max) {
		switch (opType) {
		case SQLType::OP_LE:
			cond = Condition::createBool(true);
			break;
		case SQLType::OP_GT:
			cond = Condition::createBool(false);
			break;
		case SQLType::OP_GE:
			if (!rangePreferred) {
				cond.opType_ = SQLType::OP_EQ;
			}
			break;
		default:
			break;
		}
	}
	else if (value < min) {
		switch (opType) {
		case SQLType::OP_LT:
		case SQLType::OP_LE:
		case SQLType::OP_EQ:
			cond = Condition::createBool(false);
			break;
		case SQLType::OP_GT:
		case SQLType::OP_GE:
		case SQLType::OP_NE:
			cond = Condition::createBool(true);
			break;
		default:
			break;
		}
	}
	else if (value > max) {
		switch (opType) {
		case SQLType::OP_LT:
		case SQLType::OP_LE:
		case SQLType::OP_NE:
			cond = Condition::createBool(true);
			break;
		case SQLType::OP_GT:
		case SQLType::OP_GE:
		case SQLType::OP_EQ:
			cond = Condition::createBool(false);
			break;
		default:
			break;
		}
	}

	return (cond.opType_ != SQLType::EXPR_CONSTANT);
}

template<TupleColumnType T>
void SQLExprs::IndexSelector::applyFloatingBounds(Condition &cond) {
	typedef typename SQLValues::TypeUtils::Traits<T>::ValueType ValueType;

	ValueType value;
	const bool strict = false;
	if (!SQLValues::ValueUtils::toFloatingDetail(
			cond.valuePair_.first, value, strict)) {
		cond = Condition();
		return;
	}

	cond.valuePair_.first = TupleValue(value);
}

void SQLExprs::IndexSelector::applyTimestampBounds(
		Condition &cond, TupleColumnType columnType,
		SQLValues::ValueSetHolder &valuesHolder) {
	TupleValue &condValueRef = cond.valuePair_.first;
	if (condValueRef.getType() == columnType) {
		return;
	}

	SQLValues::ValueContext cxt(SQLValues::ValueContext::ofVarContext(
			valuesHolder.getVarContext(), NULL, true));
	TupleValue adjustedValue =
			SQLValues::ValuePromoter(columnType)(&cxt, condValueRef);

	if (compareValue(adjustedValue, condValueRef) != 0) {
		ExprType &opType = cond.opType_;
		switch (opType) {
		case SQLType::OP_LT:
			opType = SQLType::OP_LE;
			break;
		case SQLType::OP_GT:
			opType = SQLType::OP_GE;
			break;
		default:
			break;
		}
	}

	condValueRef = adjustedValue;
	valuesHolder.add(adjustedValue);
}

bool SQLExprs::IndexSelector::selectClosestIndex(
		ConditionList::const_iterator condBegin,
		ConditionList::const_iterator condEnd,
		size_t acceptedCondCount, size_t &condOrdinal,
		IndexSpecId &specId) {
	condOrdinal = std::numeric_limits<size_t>::max();
	specId = std::numeric_limits<IndexSpecId>::max();

	for (ConditionList::const_iterator condIt = condBegin + acceptedCondCount;
			condIt != condEnd; ++condIt) {
		IndexMatchList &matchList = indexMatchList_;
		matchList.clear();

		if (!getAvailableIndex(*condIt, false, NULL, &specId, &matchList)) {
			continue;
		}

		bool reducible =
				reduceIndexMatch(condIt, condIt + 1, 0, matchList, 0);

		for (IndexMatchList::iterator it = matchList.begin();
				it != matchList.end(); ++it) {
			it->condOrdinal_ = static_cast<size_t>(condIt - condBegin);
		}

		for (size_t columnOrdinal = 1; reducible; columnOrdinal++) {
			reducible = reduceIndexMatch(
					condBegin, condEnd, acceptedCondCount, matchList,
					columnOrdinal);
		}

		if (matchList.empty()) {
			continue;
		}

		std::sort(matchList.begin(), matchList.end());

		condOrdinal = static_cast<size_t>(condIt - condBegin);
		specId = matchList.front().specId_;
		return true;
	}

	return false;
}

bool SQLExprs::IndexSelector::reduceIndexMatch(
		ConditionList::const_iterator condBegin,
		ConditionList::const_iterator condEnd,
		size_t acceptedCondCount, IndexMatchList &matchList,
		size_t columnOrdinal) const {
	const size_t lastMatchCount = matchList.size();
	for (size_t i = 0; i < lastMatchCount; i++) {
		IndexMatch &lastMatch = matchList[i];

		if (lastMatch.specId_ <= 0) {
			lastMatch.completed_ = true;

			if (columnOrdinal == 0) {
				matchList.push_back(lastMatch);
			}
			continue;
		}

		const IndexSpec &spec =
				specList_[static_cast<size_t>(lastMatch.specId_ - 1)];
		if (columnOrdinal >= spec.getColumnCount()) {
			lastMatch.completed_ = true;
			continue;
		}
		const uint32_t column = spec.getColumn(columnOrdinal);

		IndexMatch match = lastMatch;
		size_t nextCondOrdinal;
		if (!selectClosestIndexColumn(
				condBegin, condEnd, acceptedCondCount, match.condOrdinal_,
				column, match.specId_, nextCondOrdinal)) {
			continue;
		}

		if (columnOrdinal > 0) {
			const size_t closestCondOrdinal =
					static_cast<size_t>(std::max<uint64_t>(
							match.condOrdinal_ + 1, acceptedCondCount));

			if (nextCondOrdinal >= closestCondOrdinal) {
				match.penalty_ += nextCondOrdinal - closestCondOrdinal;
			}
			else {
				match.penalty_ += closestCondOrdinal - nextCondOrdinal;
			}

			if (nextCondOrdinal < acceptedCondCount) {
				match.penalty_ += (condEnd - condBegin);
			}
		}

		match.condOrdinal_ = nextCondOrdinal;
		matchList.push_back(match);
	}

	if (matchList.size() > lastMatchCount) {
		matchList.erase(matchList.begin(), matchList.begin() + lastMatchCount);
		return true;
	}

	if (columnOrdinal == 0) {
		matchList.clear();
	}

	return false;
}

bool SQLExprs::IndexSelector::selectClosestIndexColumn(
		ConditionList::const_iterator condBegin,
		ConditionList::const_iterator condEnd,
		size_t acceptedCondCount, size_t condOrdinal, uint32_t column,
		IndexSpecId specId, size_t &nextCondOrdinal) const {
	assert(condOrdinal < static_cast<size_t>(condEnd - condBegin));

	const ConditionList::const_iterator startIt =
			condBegin + static_cast<size_t>(std::max<uint64_t>(
					acceptedCondCount, condOrdinal));

	ConditionList::const_iterator forwardIt = condEnd;

	for (ConditionList::const_iterator it = startIt; it != condEnd; ++it) {
		if (matchIndexColumn(*it, column, specId)) {
			forwardIt = it;
			break;
		}
	}

	ConditionList::const_iterator backwardIt = condEnd;
	for (ConditionList::const_iterator it = startIt; it != condBegin;) {
		--it;
		if (matchIndexColumn(*it, column, specId)) {
			backwardIt = it;
			break;
		}
	}

	ConditionList::const_iterator matchIt = condEnd;
	if (forwardIt == condEnd) {
		if (backwardIt != condEnd) {
			matchIt = backwardIt;
		}
	}
	else {
		if (backwardIt == condEnd ||
				forwardIt - startIt <= startIt - backwardIt ||
				static_cast<size_t>(backwardIt - condBegin) <
						acceptedCondCount) {
			matchIt = forwardIt;
		}
		else {
			matchIt = backwardIt;
		}
	}

	if (matchIt == condEnd) {
		nextCondOrdinal = std::numeric_limits<size_t>::max();
		return false;
	}
	else {
		nextCondOrdinal = static_cast<size_t>(matchIt - condBegin);
		return true;
	}
}

bool SQLExprs::IndexSelector::matchIndexColumn(
		const Condition &cond, uint32_t column, IndexSpecId specId) const {
	assert(cond.column_ != Condition::EMPTY_IN_COLUMN);
	if (cond.column_ != column) {
		return false;
	}

	Condition localCond = cond;
	localCond.specId_ = specId;

	const bool withSpec = true;
	return getAvailableIndex(localCond, withSpec, NULL, NULL, NULL);
}

bool SQLExprs::IndexSelector::getAvailableIndex(
		const Condition &cond, bool withSpec, SQLType::IndexType *indexType,
		IndexSpecId *specId, IndexMatchList *matchList,
		bool topColumnOnly, bool treeIndexExtended) const {
	assert(completed_);
	assert((!withSpec) == (specId != NULL));

	if (indexType != NULL) {
		*indexType = SQLType::IndexType();
	}
	if (specId != NULL) {
		*specId = IndexSpecId();
	}

	if (cond.column_ == Condition::EMPTY_IN_COLUMN) {
		return false;
	}

	bool found = false;
	bool composite = false;
	SQLType::IndexType foundType = SQLType::IndexType();
	for (size_t i = 0; i < 2; i++) {
		const bool forComposite = (i != 0);
		const bool condComposite = (cond.specId_ != 0);

		if (withSpec && forComposite != condComposite) {
			continue;
		}

		const IndexFlags flags = indexList_[cond.column_].get(forComposite ?
				(topColumnOnly ?
						IndexFlagsEntry::FLAGS_TYPE_COMPOSITE_TOP :
						IndexFlagsEntry::FLAGS_TYPE_COMPOSITE_ALL) :
				IndexFlagsEntry::FLAGS_TYPE_SINGLE);
		if (flags == 0) {
			continue;
		}

		bool foundLocal = false;
		if (cond.opType_ == SQLType::EXPR_BETWEEN) {
			if (((flags & (1 << SQLType::INDEX_TREE_RANGE)) != 0 ||
					(treeIndexExtended &&
							(flags & (1 << SQLType::INDEX_TREE_EQ)) != 0)) &&
					(cond.isBinded() || !bulkGrouping_)) {
				foundType = SQLType::INDEX_TREE_RANGE;
				foundLocal = true;
			}
		}
		else if (cond.opType_ == SQLType::OP_EQ) {
			if ((flags & (1 << SQLType::INDEX_TREE_EQ)) != 0) {
				foundType = SQLType::INDEX_TREE_EQ;
				foundLocal = true;
			}
			else if ((flags & (1 << SQLType::INDEX_HASH)) != 0) {
				foundType = SQLType::INDEX_HASH;
				foundLocal = true;
			}
		}

		if (foundLocal) {
			found = true;
			composite = forComposite;

			if (matchList == NULL) {
				break;
			}

			if (!forComposite) {
				IndexMatch match;
				matchList->push_back(match);
			}
		}
	}

	if (!found) {
		return false;
	}

	if (withSpec && cond.specId_ != 0) {
		if (cond.specId_ > specList_.size()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		if (!specList_[static_cast<size_t>(cond.specId_ - 1)].available_) {
			return false;
		}
	}

	if (!withSpec && composite) {
		ColumnSpecIdList::const_iterator startIt = std::lower_bound(
				columnSpecIdList_.begin(), columnSpecIdList_.end(),
				ColumnSpecId(cond.column_, IndexSpecId()));

		bool foundLocal = false;
		for (ColumnSpecIdList::const_iterator it = startIt;; ++it) {
			if (it == columnSpecIdList_.end() || it->first != cond.column_) {
				return foundLocal;
			}
			const IndexSpec &spec =
					specList_[static_cast<size_t>(it->second - 1)];
			if (spec.available_) {
				foundLocal = true;

				if (specId != NULL) {
					*specId = it->second;
				}

				if (matchList == NULL) {
					break;
				}

				IndexMatch match;
				match.specId_ = it->second;
				match.spec_ = &spec;
				matchList->push_back(match);
			}
		}
	}

	if (indexType != NULL) {
		*indexType = foundType;
	}

	return true;
}

bool SQLExprs::IndexSelector::findColumn(
		const Expression &expr, uint32_t &column, bool forScan) const {
	column = std::numeric_limits<uint32_t>::max();

	if (expr.getCode().getType() != SQLType::EXPR_COLUMN) {
		return false;
	}

	const uint32_t input = expr.getCode().getInput();
	if ((forScan && input != 0) || (!forScan && input == 0)) {
		return false;
	}

	column = expr.getCode().getColumnPos();
	return true;
}

const TupleValue* SQLExprs::IndexSelector::findConst(
		const Expression &expr) {
	if (expr.getCode().getType() != SQLType::EXPR_CONSTANT) {
		return NULL;
	}

	return &expr.getCode().getValue();
}

bool SQLExprs::IndexSelector::isTrueValue(
		const TupleValue &value, bool negative, bool &nullFound) {
	nullFound = false;

	if (SQLValues::ValueUtils::isNull(value)) {
		nullFound = true;
		return false;
	}

	bool boolValue = SQLValues::ValueUtils::isTrue(value);
	if (negative) {
		boolValue = !boolValue;
	}
	return boolValue;
}

SQLExprs::IndexSpec SQLExprs::IndexSelector::createIndexSpec(
		const util::Vector<uint32_t> &columnList) {
	util::AllocUniquePtr< util::AllocVector<uint32_t> > ptr(ALLOC_UNIQUE(
			stdAlloc_, util::AllocVector<uint32_t>,
			columnList.begin(), columnList.end(), stdAlloc_));

	allColumnsList_.push_back(ptr.get());
	const util::AllocVector<uint32_t> &list = *ptr.release();

	IndexSpec spec;
	spec.columnCount_ = list.size();
	spec.columnList_ = &list[0];

	return spec;
}

void SQLExprs::IndexSelector::clearIndexColumns() {
	while (!allColumnsList_.empty()) {
		util::AllocUniquePtr< util::AllocVector<uint32_t> > ptr(
				allColumnsList_.back(), stdAlloc_);
		allColumnsList_.pop_back();
	}
}

template<typename Pair>
typename SQLExprs::IndexSelector::PairFirst<Pair>::Type&
SQLExprs::IndexSelector::getPairElement(size_t ordinal, Pair &src) {
	if (ordinal == 0) {
		return src.first;
	}
	else {
		return src.second;
	}
}

int32_t SQLExprs::IndexSelector::compareValue(
		const TupleValue &value1, const TupleValue &value2) {
	const bool sensitive = true;
	return SQLValues::ValueComparator::compareValues(
			value1, value2, sensitive, true);
}


SQLExprs::IndexSelector::IndexFlagsEntry::IndexFlagsEntry() {
	std::fill(elems_, elems_ + END_FLAGS_TYPE, FlagsElement());
}

SQLExprs::IndexSelector::IndexFlags
SQLExprs::IndexSelector::IndexFlagsEntry::get(
		IndexFlagsType type) const {
	return elems_[type];
}

void SQLExprs::IndexSelector::IndexFlagsEntry::add(
		IndexFlagsType type, IndexFlags flags) {
	assert(flags <= std::numeric_limits<FlagsElement>::max());

	FlagsElement &elem = elems_[type];
	elem = static_cast<FlagsElement>(elem | flags);
}


SQLExprs::IndexSelector::IndexMatch::IndexMatch() :
		specId_(0),
		spec_(NULL),
		condOrdinal_(0),
		penalty_(0),
		completed_(false) {
}

bool SQLExprs::IndexSelector::IndexMatch::operator<(
		const IndexMatch &another) const {
	if (!completed_ != !another.completed_) {
		return completed_;
	}

	if (penalty_ != another.penalty_) {
		return penalty_ < another.penalty_;
	}

	if ((spec_ == NULL) != (another.spec_ == NULL)) {
		return (spec_ == NULL);
	}
	else if (spec_ != NULL && another.spec_ != NULL &&
			!(*spec_ == *another.spec_)) {
		return *spec_ < *another.spec_;
	}

	if (condOrdinal_ != another.condOrdinal_) {
		return condOrdinal_ < another.condOrdinal_;
	}

	return specId_ < another.specId_;
}


inline SQLExprs::IndexSelector::BulkPosition::BulkPosition(size_t pos) :
		pos_(pos) {
}

inline size_t SQLExprs::IndexSelector::BulkPosition::get() const {
	return pos_;
}

inline bool SQLExprs::IndexSelector::BulkPosition::operator<(
		const BulkPosition &another) const {
	return BulkRewriter::comparePosition(*this, another) < 0;
}


SQLExprs::IndexSelector::BulkRewriter::BulkRewriter() {
}

void SQLExprs::IndexSelector::BulkRewriter::rewrite(
		ConditionList &condList) const {
	util::StackAllocator &alloc = getAllocator(condList);
	util::StackAllocator::Scope scope(alloc);

	PositionList posList(alloc);
	generateInitialPositions(condList, posList);
	arrangeBulkPositions(condList, posList);
	applyBulkPositions(posList, condList);
}

int32_t SQLExprs::IndexSelector::BulkRewriter::comparePosition(
		const BulkPosition &pos1, const BulkPosition &pos2) {
	return compareUIntValue(pos1.get(), pos2.get());
}

int32_t SQLExprs::IndexSelector::BulkRewriter::compareBulkCondition(
		const ConditionList &condList, const BulkPosition &pos1,
		const BulkPosition &pos2) {
	int32_t comp;
	do {
		if ((comp = compareBulkTarget(condList, pos1, pos2)) != 0) {
			break;
		}
		if (!isBulkTarget(condList, pos1)) {
			comp = comparePosition(pos1, pos2);
			break;
		}

		if ((comp = compareBulkSpec(condList, pos1, pos2)) != 0) {
			break;
		}
		if ((comp = compareBulkValue(condList, pos1, pos2)) != 0) {
			break;
		}
		comp = comparePosition(pos1, pos2);
	}
	while (false);
	return comp;
}

void SQLExprs::IndexSelector::BulkRewriter::generateInitialPositions(
		const ConditionList &condList, PositionList &posList) {
	assert(posList.empty());
	for (size_t i = 0; i < condList.size();) {
		const BulkPosition pos(i);
		const size_t size = getBulkUnitSize(condList, pos);

		posList.push_back(pos);
		i += size;
	}
}

void SQLExprs::IndexSelector::BulkRewriter::arrangeBulkPositions(
		const ConditionList &condList, PositionList &posList) {
	PositionList basePosList = posList;
	posList.clear();

	std::sort(
			basePosList.begin(), basePosList.end(),
			BulkConditionLess(condList));

	PositionPairList topPosList(getAllocator(posList));
	for (size_t offset = 0; offset < basePosList.size();) {
		const size_t count =
				getGroupPositionCount(condList, basePosList, offset);
		BulkPosition topPos = basePosList[offset];
		for (size_t i = 1; i < count; i++) {
			const BulkPosition &subPos = basePosList[offset + i];
			if (comparePosition(subPos, topPos) < 0) {
				topPos = subPos;
			}
		}

		assert(offset < basePosList.size());
		const PositionPair posToOffset(topPos, BulkPosition(offset));

		topPosList.push_back(posToOffset);
		offset += count;
	}
	std::sort(topPosList.begin(), topPosList.end());

	for (PositionPairList::const_iterator it = topPosList.begin();
			it != topPosList.end(); ++it) {
		const size_t offset = it->second.get();
		const size_t count =
				getGroupPositionCount(condList, basePosList, offset);
		posList.insert(
				posList.end(),
				basePosList.begin() + offset,
				basePosList.begin() + offset + count);
	}
}

void SQLExprs::IndexSelector::BulkRewriter::applyBulkPositions(
		const PositionList &posList, ConditionList &condList) {
	ConditionList baseCondList = condList;
	condList.clear();

	size_t skipSize = 0;
	size_t lastGroupOffset = 0;
	for (PositionList::const_iterator it = posList.begin();
			it != posList.end(); ++it) {
		const BulkPosition &pos = *it;
		const size_t size = getBulkUnitSize(baseCondList, pos);

		if (it != posList.begin()) {
			const BulkPosition &prevPos = *(it - 1);
			if (!isSameGroup(baseCondList, prevPos, pos)) {
				fillGroupOrdinals(condList, lastGroupOffset);
				lastGroupOffset = condList.size();
			}
			else if (compareBulkValue(baseCondList, prevPos, pos) == 0) {
				skipSize += size;
				continue;
			}
		}

		condList.insert(
				condList.end(),
				baseCondList.begin() + pos.get(),
				baseCondList.begin() + pos.get() + size);
	}

	fillGroupOrdinals(condList, lastGroupOffset);

	if (condList.size() + skipSize != baseCondList.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

int32_t SQLExprs::IndexSelector::BulkRewriter::compareBulkTarget(
		const ConditionList &condList, const BulkPosition &pos1,
		const BulkPosition &pos2) {
	const int32_t value1 = (isBulkTarget(condList, pos1) ? 1 : 0);
	const int32_t value2 = (isBulkTarget(condList, pos2) ? 1 : 0);
	return (value1 - value2);
}

int32_t SQLExprs::IndexSelector::BulkRewriter::compareBulkSpec(
		const ConditionList &condList, const BulkPosition &pos1,
		const BulkPosition &pos2) {
	assert(isBulkTarget(condList, pos1));
	assert(isBulkTarget(condList, pos2));

	int32_t comp;
	do {
		const Condition &cond1 = getCondition(condList, pos1);
		const Condition &cond2 = getCondition(condList, pos2);
		if ((comp = compareUIntValue(cond1.specId_, cond2.specId_)) != 0) {
			break;
		}
		if ((comp = compareUIntValue(cond1.column_, cond2.column_)) != 0) {
			break;
		}
		comp = compareUIntValue(
				getBulkUnitSize(condList, pos1),
				getBulkUnitSize(condList, pos2));
	}
	while (false);
	return comp;
}

int32_t SQLExprs::IndexSelector::BulkRewriter::compareBulkValue(
		const ConditionList &condList, const BulkPosition &pos1,
		const BulkPosition &pos2) {
	assert(compareBulkSpec(condList, pos1, pos2) == 0);

	const size_t size = getBulkUnitSize(condList, pos1);
	int32_t comp;
	do {
		for (size_t i = 0; i < size; i++) {
			const Condition &cond1 = getCondition(condList, pos1.get() + i);
			const Condition &cond2 = getCondition(condList, pos2.get() + i);

			if ((comp = compareUIntValue(
					cond1.column_, cond2.column_)) != 0) {
				break;
			}

			const TupleValue &value1 = cond1.valuePair_.first;
			const TupleValue &value2 = cond2.valuePair_.first;
			if ((comp = compareValue(value1, value2)) != 0) {
				break;
			}
		}
	}
	while (false);
	return comp;
}

int32_t SQLExprs::IndexSelector::BulkRewriter::compareUIntValue(
		uint64_t value1, uint64_t value2) {
	return SQLValues::ValueUtils::compareRawValue(value1, value2);
}

bool SQLExprs::IndexSelector::BulkRewriter::isBulkTarget(
		const ConditionList &condList, const BulkPosition &pos) {
	const size_t size = getBulkUnitSize(condList, pos);

	for (size_t i = 0; i < size; i++) {
		const Condition &cond = getCondition(condList, pos.get() + i);
		if (cond.compositeAndCount_ > 1 || cond.opType_ != SQLType::OP_EQ ||
				!cond.isBinded() || cond.firstHitOnly_ || cond.descending_) {
			return false;
		}
	}

	return true;
}

size_t SQLExprs::IndexSelector::BulkRewriter::getBulkUnitSize(
		const ConditionList &condList, const BulkPosition &pos) {
	assert(pos.get() < condList.size());

	const size_t size = nextOrConditionDistance(
			condList.begin() + pos.get(), condList.end());
	assert(size > 0);

	return size;
}

void SQLExprs::IndexSelector::BulkRewriter::fillGroupOrdinals(
		ConditionList &condList, size_t offset) {
	assert(offset <= condList.size());

	size_t orCount = 0;
	for (size_t i = offset; i < condList.size();) {
		orCount++;
		i += getBulkUnitSize(condList, BulkPosition(i));
	}

	size_t rest = 0;
	size_t orOrdinal = 0;
	for (size_t i = offset; i < condList.size(); i++) {
		if (rest == 0) {
			rest = getBulkUnitSize(condList, BulkPosition(i));
			if (i != offset) {
				orOrdinal++;
			}
		}
		rest--;

		assert(orCount > 0);

		Condition &cond = condList[i];
		cond.bulkOrCount_ = orCount;
		cond.bulkOrOrdinal_ = orOrdinal;
	}
}

size_t SQLExprs::IndexSelector::BulkRewriter::getGroupPositionCount(
		const ConditionList &condList, const PositionList &posList,
		size_t offset) {
	assert(offset < posList.size());

	size_t i = offset;
	while (++i < posList.size()) {
		const BulkPosition &pos1 = posList[i - 1];
		const BulkPosition &pos2 = posList[i];
		if (!isSameGroup(condList, pos1, pos2)) {
			break;
		}
	}

	return i - offset;
}

bool SQLExprs::IndexSelector::BulkRewriter::isSameGroup(
		const ConditionList &condList, const BulkPosition &pos1,
		const BulkPosition &pos2) {
	return (
			isBulkTarget(condList, pos1) &&
			isBulkTarget(condList, pos2) &&
			compareBulkSpec(condList, pos1, pos2) == 0);
}

const SQLExprs::IndexSelector::Condition&
SQLExprs::IndexSelector::BulkRewriter::getCondition(
		const ConditionList &condList, const BulkPosition &pos) {
	return getCondition(condList, pos.get());
}

const SQLExprs::IndexSelector::Condition&
SQLExprs::IndexSelector::BulkRewriter::getCondition(
		const ConditionList &condList, size_t pos) {
	assert(pos < condList.size());
	return condList[pos];
}

template<typename T>
util::StackAllocator& SQLExprs::IndexSelector::BulkRewriter::getAllocator(
		util::Vector<T> &src) {
	util::StackAllocator *alloc = src.get_allocator().base();
	assert(alloc != NULL);
	return *alloc;
}


SQLExprs::IndexSelector::BulkConditionLess::BulkConditionLess(
		const ConditionList &condList) :
		condList_(condList) {
}

bool SQLExprs::IndexSelector::BulkConditionLess::operator()(
		const BulkPosition &pos1, const BulkPosition &pos2) const {
	return (BulkRewriter::compareBulkCondition(condList_, pos1, pos2) < 0);
}


SQLExprs::IndexSelector::AggregationMatcher::AggregationMatcher(
		const util::StdAllocator<void, void> &alloc,
		const IndexSelector &base,
		SQLValues::ValueSetHolder &valuesHolder) :
		matcherAlloc_(alloc),
		base_(base),
		valuesHolder_(valuesHolder) {
}

bool SQLExprs::IndexSelector::AggregationMatcher::match(
		const Expression &proj, const Expression *pred,
		IndexConditionList &condList) {
	condList.clear();

	AllocIndexConditionList localCondList(matcherAlloc_);
	AggregationKeySet keySet(matcherAlloc_);

	if (!matchProjection(proj, localCondList, keySet, true)) {
		return false;
	}

	if (pred != NULL && !matchPredicate(*pred, localCondList, false)) {
		return false;
	}

	condList.assign(localCondList.begin(), localCondList.end());
	return true;
}

bool SQLExprs::IndexSelector::AggregationMatcher::matchProjection(
		const Expression &expr, AllocIndexConditionList &condList,
		AggregationKeySet &keySet, bool top) {
	if (!top) {
		const ExprType exprType = expr.getCode().getType();
		if (ExprTypeUtils::isAggregation(exprType)) {
			if (matchAggregationExpr(expr, condList, keySet)) {
				return true;
			}
			return false;
		}

		if (exprType == SQLType::EXPR_CONSTANT) {
			return true;
		}
		else if (exprType == SQLType::EXPR_COLUMN) {
			return false;
		}
	}

	for (Expression::Iterator it(expr); it.exists(); it.next()) {
		if (!matchProjection(it.get(), condList, keySet, false)) {
			return false;
		}
	}

	if (top && condList.empty()) {
		return false;
	}

	return true;
}

bool SQLExprs::IndexSelector::AggregationMatcher::matchPredicate(
		const Expression &expr, AllocIndexConditionList &condList,
		bool negative) {
	const ExprCode &code = expr.getCode();
	const ExprType exprType = code.getType();

	if (exprType == SQLType::EXPR_CONSTANT) {
		bool nullFound;
		return isTrueValue(code.getValue(), negative, nullFound);
	}
	else if (exprType ==
			ExprTypeUtils::getLogicalOp(SQLType::EXPR_AND, negative)) {
		for (Expression::Iterator it(expr); it.exists(); it.next()) {
			if (!matchPredicate(it.get(), condList, negative)) {
				return false;
			}
		}
		return true;
	}
	else if (exprType == SQLType::OP_NOT) {
		return matchPredicate(expr.child(), condList, !negative);
	}
	else {
		return matchValueCondition(expr, condList, negative);
	}
}

bool SQLExprs::IndexSelector::AggregationMatcher::matchAggregationExpr(
		const Expression &expr, AllocIndexConditionList &condList,
		AggregationKeySet &keySet) {
	bool descending;
	if (!matchAggregationType(expr, descending) || expr.getChildCount() != 1) {
		return false;
	}

	Condition cond;
	if (!base_.findColumn(expr.child(), cond.column_, true)) {
		return false;
	}
	cond.opType_ = SQLType::EXPR_BETWEEN;

	const bool topColumnOnly = true;
	const bool treeIndexExtended = true;
	IndexSpecId specId;
	if (!base_.getAvailableIndex(
			cond, false, NULL, &specId, NULL, topColumnOnly,
			treeIndexExtended)) {
		return false;
	}

	cond.firstHitOnly_ = true;
	cond.descending_ = descending;
	cond.specId_ = specId;

	const bool inserted =
			keySet.insert(AggregationKey(cond.column_, descending)).second;
	if (inserted) {
		condList.push_back(cond);
	}
	return true;
}

bool SQLExprs::IndexSelector::AggregationMatcher::matchAggregationType(
		const Expression &expr, bool &descending) {
	descending = false;

	switch (expr.getCode().getType()) {
	case SQLType::AGG_MIN:
		break;
	case SQLType::AGG_MAX:
		descending = true;
		break;
	default:
		return false;
	}
	return true;
}

bool SQLExprs::IndexSelector::AggregationMatcher::matchValueCondition(
		const Expression &expr, AllocIndexConditionList &condList,
		bool negative) {
	bool placeholderAffected = false;
	const bool topColumnOnly = true;
	const bool treeIndexExtended = true;
	const Condition cond = base_.makeValueCondition(
			expr, negative, placeholderAffected, valuesHolder_, topColumnOnly,
			treeIndexExtended);

	if (cond.isEmpty()) {
		return false;
	}

	if (cond.opType_ == SQLType::EXPR_CONSTANT) {
		return cond.equals(true);
	}

	for (AllocIndexConditionList::iterator it = condList.begin();
			it != condList.end(); ++it) {
		Condition destCond;
		if (!base_.tryMakeNarrower(*it, cond, destCond) ||
				destCond.isEmpty() ||
				destCond.opType_ == SQLType::EXPR_CONSTANT) {
			return false;
		}
		*it = destCond;
	}

	return true;
}


bool SQLExprs::ExprTypeUtils::isAggregation(ExprType type) {
	return (SQLType::START_AGG < type && type < SQLType::END_AGG);
}

bool SQLExprs::ExprTypeUtils::isFunction(ExprType type) {
	return (SQLType::START_FUNC < type && type < SQLType::END_FUNC);
}

bool SQLExprs::ExprTypeUtils::isNoColumnTyped(ExprType type) {
	return (type == SQLType::EXPR_LIST ||
			type == SQLType::EXPR_PROJECTION ||
			type == SQLType::EXPR_SELECTION ||
			type == SQLType::EXPR_PRODUCTION ||
			type == SQLType::EXPR_TYPE);
}

bool SQLExprs::ExprTypeUtils::isCompOp(ExprType type, bool withNe) {
	if (negateCompOp(type) == type) {
		return false;
	}

	if (type == SQLType::OP_NE && withNe) {
		return false;
	}

	return true;
}

bool SQLExprs::ExprTypeUtils::isNormalWindow(ExprType type) {
	const ExprSpec &spec = ExprFactory::getDefaultFactory().getSpec(type);
	return ((spec.flags_ & ExprSpec::FLAG_WINDOW) != 0);
}

bool SQLExprs::ExprTypeUtils::isDecremental(ExprType type) {
	const ExprSpec &spec = ExprFactory::getDefaultFactory().getSpec(type);
	return ((spec.flags_ & ExprSpec::FLAG_AGGR_DECREMENTAL) != 0);
}

SQLExprs::ExprType SQLExprs::ExprTypeUtils::swapCompOp(ExprType type) {
	switch (type) {
	case SQLType::OP_LT:
		return SQLType::OP_GT;
	case SQLType::OP_GT:
		return SQLType::OP_LT;
	case SQLType::OP_LE:
		return SQLType::OP_GE;
	case SQLType::OP_GE:
		return SQLType::OP_LE;
	default:
		return type;
	}
}

SQLExprs::ExprType SQLExprs::ExprTypeUtils::negateCompOp(ExprType type) {
	switch (type) {
	case SQLType::OP_LT:
		return SQLType::OP_GE;
	case SQLType::OP_GT:
		return SQLType::OP_LE;
	case SQLType::OP_LE:
		return SQLType::OP_GT;
	case SQLType::OP_GE:
		return SQLType::OP_LT;
	case SQLType::OP_EQ:
		return SQLType::OP_NE;
	case SQLType::OP_NE:
		return SQLType::OP_EQ;
	default:
		return type;
	}
}

SQLExprs::ExprType SQLExprs::ExprTypeUtils::getLogicalOp(
		ExprType type, bool negative) {
	if (negative) {
		switch (type) {
		case SQLType::EXPR_AND:
			return SQLType::EXPR_OR;
		case SQLType::EXPR_OR:
			return SQLType::EXPR_AND;
		default:
			break;
		}
	}

	return type;
}

SQLValues::CompColumn SQLExprs::ExprTypeUtils::toCompColumn(
		ExprType type, uint32_t pos1, uint32_t pos2, bool last) {
	SQLValues::CompColumn column;

	column.setColumnPos(pos1, true);
	column.setColumnPos(pos2, false);

	if (type == SQLType::OP_EQ) {
		column.setOrdering(false);
	}

	if (!last) {
		return column;
	}

	bool exclusive = false;
	bool less = false;
	switch (type) {
	case SQLType::OP_LT:
		exclusive = true;
		less = true;
		break;
	case SQLType::OP_GT:
		exclusive = true;
		break;
	case SQLType::OP_LE:
		less = true;
		break;
	case SQLType::OP_GE:
		break;
	default:
		return column;
	}

	column.setEitherUnbounded(true);
	column.setLowerUnbounded(less);
	column.setEitherExclusive(exclusive);
	column.setLowerExclusive(exclusive && !less);

	return column;
}

SQLExprs::ExprType SQLExprs::ExprTypeUtils::getCompColumnOp(
		const SQLValues::CompColumn &column) {
	if (!column.isEitherUnbounded()) {
		return SQLType::OP_EQ;
	}

	if (column.isEitherExclusive()) {
		if (column.isLowerUnbounded()) {
			return SQLType::OP_LT;
		}
		else {
			return SQLType::OP_GT;
		}
	}
	else {
		if (column.isLowerUnbounded()) {
			return SQLType::OP_LE;
		}
		else {
			return SQLType::OP_GE;
		}
	}
}

bool SQLExprs::ExprTypeUtils::isVarNonGenerative(
		ExprType type, bool forLob, bool forLargeFixed) {
	switch (type) {
	case SQLType::EXPR_CONSTANT:
		return true;
	case SQLType::EXPR_COLUMN:
		if (forLob) {
			break;
		}
		return true;
	case SQLType::EXPR_CASE:
	case SQLType::FUNC_COALESCE:
	case SQLType::FUNC_IFNULL:
	case SQLType::FUNC_MAX:
	case SQLType::FUNC_MIN:
	case SQLType::FUNC_NULLIF:
		if (forLargeFixed) {
			break;
		}
		return true;
	default:
		break;
	}

	return false;
}


SQLExprs::PlanPartitioningInfo::PlanPartitioningInfo() :
		partitioningType_(SyntaxTree::TABLE_PARTITION_TYPE_UNDEF),
		partitioningColumnId_(-1),
		subPartitioningColumnId_(-1),
		partitioningCount_(0),
		clusterPartitionCount_(0),
		intervalValue_(0),
		nodeAffinityList_(NULL),
		availableList_(NULL) {
}


SQLExprs::PlanNarrowingKey::PlanNarrowingKey() :
		longRange_(getRangeFull()),
		hashIndex_(0),
		hashCount_(0) {
}

void SQLExprs::PlanNarrowingKey::setNone() {
	*this = PlanNarrowingKey();
	longRange_ = getRangeNone();
}

void SQLExprs::PlanNarrowingKey::setRange(const LongRange &longRange) {
	longRange_ = longRange;
}

void SQLExprs::PlanNarrowingKey::setHash(
		uint32_t hashIndex, uint32_t hashCount) {
	hashIndex_ = hashIndex;
	hashCount_ = hashCount;
}

SQLExprs::PlanNarrowingKey::LongRange
SQLExprs::PlanNarrowingKey::getRangeFull() {
	return LongRange(
			std::numeric_limits<LongRange::first_type>::min(),
			std::numeric_limits<LongRange::second_type>::max());
}

SQLExprs::PlanNarrowingKey::LongRange 
SQLExprs::PlanNarrowingKey::getRangeNone() {
	return LongRange(0, -1);
}


int64_t SQLExprs::DataPartitionUtils::intervalToAffinity(
		const PlanPartitioningInfo &partitioning, int64_t interval,
		uint32_t hash) {
	return intervalToAffinity(
			partitioning.partitioningType_,
			partitioning.partitioningCount_,
			partitioning.clusterPartitionCount_,
			*partitioning.nodeAffinityList_,
			interval, hash);
}

std::pair<int64_t, uint32_t>
SQLExprs::DataPartitionUtils::intervalHashFromAffinity(
		const PlanPartitioningInfo &partitioning,
		const util::Vector<uint32_t> &affinityRevList, int64_t affinity) {
	return intervalHashFromAffinity(
			partitioning.partitioningType_,
			partitioning.partitioningCount_,
			partitioning.clusterPartitionCount_,
			affinityRevList, affinity);
}

bool SQLExprs::DataPartitionUtils::isFixedIntervalPartitionAffinity(
		const PlanPartitioningInfo &partitioning, int64_t affinity) {
	return isFixedIntervalPartitionAffinity(
			partitioning.partitioningType_,
			partitioning.clusterPartitionCount_,
			affinity);
}

void SQLExprs::DataPartitionUtils::makeAffinityRevList(
		const PlanPartitioningInfo &partitioning,
		util::Vector<uint32_t> &affinityRevList) {
	makeAffinityRevList(
			partitioning.partitioningType_,
			partitioning.partitioningCount_,
			partitioning.clusterPartitionCount_,
			*partitioning.nodeAffinityList_,
			affinityRevList);
}

bool SQLExprs::DataPartitionUtils::reducePartitionedTarget(
		SQLValues::ValueContext &cxt, bool forContainer,
		const PlanPartitioningInfo *partitioning,
		const util::Vector<TupleColumnType> &columnTypeList,
		const SyntaxExpr &expr, bool &uncovered,
		util::Vector<int64_t> *affinityList, util::Set<int64_t> &affinitySet,
		const util::Vector<TupleValue> *parameterList,
		bool &placeholderAffected, util::Set<int64_t> &unmatchAffinitySet) {

	if (partitioning == NULL || (forContainer &&
			partitioning->partitioningType_ ==
					SyntaxTree::TABLE_PARTITION_TYPE_UNDEF)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
	}

	util::StackAllocator &alloc = cxt.getAllocator();
	SQLValues::ValueContext valueCxt(
			SQLValues::ValueContext::ofAllocator(alloc));

	TupleList::Info tupleInfo;
	if (!columnTypeList.empty()) {
		tupleInfo.columnCount_ = columnTypeList.size();
		tupleInfo.columnTypeList_ = &columnTypeList[0];
	}

	const bool hashOnly = (partitioning->partitioningType_ ==
			SyntaxTree::TABLE_PARTITION_TYPE_HASH);
	const bool composite = (partitioning->partitioningType_ ==
			SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH);
	const uint32_t hashCount = (!forContainer || hashOnly || composite ?
			partitioning->partitioningCount_ : 1);

	uint32_t indexType = 0;
	if (hashOnly) {
		indexType |= (1 << SQLType::INDEX_HASH);
	}
	else if (
			partitioning->partitioningType_ ==
					SyntaxTree::TABLE_PARTITION_TYPE_RANGE ||
			partitioning->partitioningType_ ==
					SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
		indexType |= (1 << SQLType::INDEX_TREE_RANGE);
		indexType |= (1 << SQLType::INDEX_TREE_EQ);
	}

	IndexSelector selector(valueCxt, SQLType::EXPR_COLUMN, tupleInfo);
	if (forContainer) {
		selector.addIndex(
				static_cast<uint32_t>(partitioning->partitioningColumnId_),
				indexType);
		selector.completeIndex();

		Expression::InOption option(valueCxt);
		option.syntaxExpr_ = &expr;

		for (size_t i = 0; i < 2; i++) {
			const bool withParameterList = (i != 0);

			option.parameterList_ = (withParameterList ? parameterList : NULL);

			ExprFactoryContext factoryCxt(alloc);
			Expression &procExpr = Expression::importFrom(factoryCxt, option);
			selector.select(procExpr);

			if (!withParameterList) {
				placeholderAffected = selector.isPlaceholderAffected();
				if (parameterList == NULL) {
					break;
				}
			}
		}
	}

	const IndexConditionList &condList = selector.getConditionList();

	typedef std::pair<int64_t, int64_t> Range;
	typedef util::Vector<Range> RangeList;

	RangeList rangeList(alloc);
	if (forContainer && selector.isSelected()) {
		for (IndexConditionList::const_iterator it = condList.begin();
				it != condList.end(); ++it) {
			if (it->opType_ != SQLType::OP_EQ &&
					it->opType_ != SQLType::EXPR_BETWEEN) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
			}
			if (!hashOnly && it->opType_ == SQLType::EXPR_BETWEEN) {
				uncovered = true;
			}
			Range range;
			for (size_t i = 0; i < 2; i++) {
				const TupleValue &condValue =
						(i == 0 ? it->valuePair_.first : it->valuePair_.second);
				int64_t &rawInterval = (i == 0 ? range.first : range.second);
				if (SQLValues::ValueUtils::isNull(condValue)) {
					rawInterval = (i == 0 ?
							std::numeric_limits<int64_t>::min() :
							std::numeric_limits<int64_t>::max());
				}
				else if (hashOnly) {
					if (partitioning->partitioningCount_ <= 0) {
						assert(false);
						GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
					}
					rawInterval =
							SQLValues::ValueFnv1aHasher::ofValue(condValue)(condValue) %
							partitioning->partitioningCount_;
				}
				else {
					const int64_t unit = partitioning->intervalValue_;
					rawInterval = rawIntervalFromValue(condValue, unit);

					if (i == 0 && !it->inclusivePair_.first &&
							rawInterval !=
									std::numeric_limits<int64_t>::max() &&
							SQLValues::ValueUtils::isUpperBoundaryAsLong(
									condValue, unit)) {
						rawInterval++;
					}
					else if (i != 0 && !it->inclusivePair_.second &&
							rawInterval !=
									std::numeric_limits<int64_t>::min() &&
							SQLValues::ValueUtils::isLowerBoundaryAsLong(
									condValue, unit)) {
						rawInterval--;
					}
				}
			}
			if (it->opType_ == SQLType::OP_EQ) {
				range.second = range.first;
			}
			rangeList.push_back(range);
		}
	}
	else if (forContainer && !hashOnly && !(condList.size() == 1 &&
			(condList[0].isNull() || condList[0].equals(false)))) {
		if (!hashOnly) {
			uncovered = true;
		}
		rangeList.push_back(Range(
				std::numeric_limits<int64_t>::min(),
				std::numeric_limits<int64_t>::max()));
	}

	uint64_t availableCount = 0;

	if (!forContainer || hashOnly) {
		if (rangeList.empty()) {
			for (uint32_t i = 0; i < partitioning->partitioningCount_; i++) {
				int64_t affinity;
				if (hashOnly) {
					affinity = intervalToAffinity(*partitioning, 0, i);
				}
				else {
					affinity = i;
				}
				affinitySet.insert(affinity);
			}
		}
		else {
			for (RangeList::const_iterator it = rangeList.begin();
					it != rangeList.end(); ++it) {
				affinitySet.insert(intervalToAffinity(
						*partitioning, 0, static_cast<uint32_t>(it->first)));
			}
		}
		availableCount += hashCount;
	}

	const size_t rangeCount = rangeList.size();

	typedef std::pair<int64_t, int64_t> OutsideEntry;
	typedef util::Vector<OutsideEntry> OutsideList;

	OutsideList outsideIntervalList(rangeCount, OutsideEntry(-1, -1), alloc);
	OutsideList outsideDistanceList(rangeCount, OutsideEntry(), alloc);
	util::Vector<bool> rangeUnmatchList(rangeCount, true, alloc);

	typedef PlanPartitioningInfo::IntervalList IntervalList;
	const IntervalList &intervalList = (hashOnly ?
			IntervalList(alloc) : *partitioning->availableList_);
	for (IntervalList::const_iterator intervalIt = intervalList.begin();
			intervalIt != intervalList.end(); ++intervalIt) {
		int64_t baseRawInterval = intervalToRaw(intervalIt->first);
		if (baseRawInterval < 0) {
			baseRawInterval++;
		}
		for (int64_t i = 0; i < intervalIt->second; i++) {
			const int64_t rawInterval = baseRawInterval + i;
			const int64_t interval = intervalFromRaw(rawInterval);
			for (RangeList::const_iterator it = rangeList.begin();
					it != rangeList.end(); ++it) {
				const ptrdiff_t i = it - rangeList.begin();
				if (rawInterval < it->first || it->second < rawInterval) {
					OutsideEntry &outsideInterval = outsideIntervalList[i];
					OutsideEntry &outsideDistance = outsideDistanceList[i];

					if (rawInterval < it->first) {
						const int64_t distance = it->first - rawInterval;
						if (outsideInterval.first < 0 ||
								distance < outsideDistance.first) {
							outsideInterval.first = interval;
							outsideDistance.first = distance;
						}
					}
					if (it->second < rawInterval) {
						const int64_t distance = rawInterval - it->second;
						if (outsideInterval.second < 0 ||
								distance < outsideDistance.second) {
							outsideInterval.second = interval;
							outsideDistance.second = distance;
						}
					}
					continue;
				}
				for (uint32_t hash = 0; hash < hashCount; hash++) {
					affinitySet.insert(
							intervalToAffinity(*partitioning, interval, hash));
				}
				rangeUnmatchList[i] = false;
			}
		}
		availableCount += intervalIt->second * hashCount;
	}

	if (!hashOnly) {
		for (util::Vector<bool>::iterator it = rangeUnmatchList.begin();
				it != rangeUnmatchList.end(); ++it) {
			if (!(*it)) {
				continue;
			}
			uncovered = true;

			const ptrdiff_t i = it - rangeUnmatchList.begin();
			const OutsideEntry &outsideInterval = outsideIntervalList[i];

			for (uint32_t hash = 0; hash < hashCount; hash++) {
				for (size_t j = 0; j < 2; j++) {
					const int64_t interval = (j == 0 ?
							outsideInterval.first : outsideInterval.second);
					if (interval < 0) {
						continue;
					}
					const int64_t affinity =
							intervalToAffinity(*partitioning, interval, hash);
					if (affinitySet.insert(affinity).second) {
						unmatchAffinitySet.insert(affinity);
					}
				}
			}
		}
	}

	if (affinityList != NULL) {
		for (util::Set<int64_t>::const_iterator it = affinitySet.begin();
				it != affinitySet.end(); ++it) {
			affinityList->push_back(*it);
		}
	}

	return affinitySet.size() < availableCount;
}

bool SQLExprs::DataPartitionUtils::isReducibleTablePartitionCondition(
		const PlanPartitioningInfo &partitioning, TupleColumnType columnType,
		const util::Vector<uint32_t> &affinityRevList, int64_t nodeAffinity,
		ExprType condType, uint32_t condColumn, const TupleValue &condValue) {
	switch (partitioning.partitioningType_) {
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
		break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
		break;
	default:
		return false;
	}

	if (static_cast<uint32_t>(partitioning.partitioningColumnId_) !=
			condColumn) {
		return false;
	}

	if (isFixedIntervalPartitionAffinity(partitioning, nodeAffinity)) {
		return false;
	}

	const SQLValues::CompColumn compOp =
			ExprTypeUtils::toCompColumn(condType, 0, 0, true);
	if (!compOp.isEitherUnbounded()) {
		return false;
	}

	if (SQLValues::ValueUtils::isNull(condValue)) {
		return false;
	}

	const int64_t unit = partitioning.intervalValue_;
	const int64_t targetInterval = intervalHashFromAffinity(
			partitioning, affinityRevList, nodeAffinity).first;

	const int64_t targetRawInterval = intervalToRaw(targetInterval);
	const int64_t condRawInterval = rawIntervalFromValue(condValue, unit);
	const int32_t intervalComp = SQLValues::ValueUtils::compareIntegral(
			targetRawInterval, condRawInterval);

	const bool lower = compOp.isLowerUnbounded();
	const bool exclusive = compOp.isEitherExclusive();
	const int32_t typeDir =
			SQLValues::ValueUtils::getTypeBoundaryDirectionAsLong(
					condValue, columnType, !lower);
	if (lower) {
		if (typeDir > 0 ||
				(!exclusive && (SQLValues::ValueUtils::isUpperBoundaryAsLong(
						condValue, unit) || typeDir == 0))) {
			return intervalComp <= 0;
		}
		else {
			return intervalComp < 0;
		}
	}
	else {
		if (typeDir < 0 ||
				(!exclusive && (SQLValues::ValueUtils::isLowerBoundaryAsLong(
						condValue, unit) || typeDir == 0))) {
			return intervalComp >= 0;
		}
		else {
			return intervalComp > 0;
		}
	}
}

void SQLExprs::DataPartitionUtils::getTablePartitionKey(
		const PlanPartitioningInfo &partitioning,
		const util::Vector<uint32_t> &affinityRevList, int64_t nodeAffinity,
		PlanNarrowingKey &key, PlanNarrowingKey &subKey, bool &subFound) {

	const SyntaxTree::TablePartitionType type = partitioning.partitioningType_;
	subFound = (type == SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH);

	const bool noRange = (nodeAffinity < partitioning.clusterPartitionCount_);

	std::pair<int64_t, uint32_t> intervalHash;
	if (!noRange) {
		intervalHash = intervalHashFromAffinity(
				partitioning, affinityRevList, nodeAffinity);
	}

	if (type == SyntaxTree::TABLE_PARTITION_TYPE_RANGE ||
			type == SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
		PlanNarrowingKey &destKey = key;
		if (noRange) {
			destKey.setNone();
		}
		else {
			const int64_t rawInterval = intervalToRaw(intervalHash.first);
			destKey.setRange(rawIntervalToLong(
					rawInterval, partitioning.intervalValue_));
		}
	}

	if (type == SyntaxTree::TABLE_PARTITION_TYPE_HASH ||
			type == SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
		PlanNarrowingKey &destKey = (subFound ? subKey : key);
		if (noRange) {
			destKey.setNone();
		}
		else {
			destKey.setHash(
					intervalHash.second, partitioning.partitioningCount_);
		}
	}
}

int64_t SQLExprs::DataPartitionUtils::intervalToRaw(int64_t interval) {
	if (interval < 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if ((interval & 0x1) == 1) {
		return (-(interval >> 1) - 1);
	}
	else {
		return (interval >> 1);
	}
}

int64_t SQLExprs::DataPartitionUtils::intervalFromRaw(int64_t rawInterval) {
	if (rawInterval < 0) {
		return ((-(rawInterval + 1)) << 1) | 1;
	}
	else {
		return rawInterval << 1;
	}
}

int64_t SQLExprs::DataPartitionUtils::rawIntervalFromValue(
		const TupleValue &value, int64_t unit) {
	return rawIntervalFromValue(intervalValueToLong(value), unit);
}

int64_t SQLExprs::DataPartitionUtils::rawIntervalFromValue(
		int64_t longValue, int64_t unit) {
	if (unit <= 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (longValue >= 0) {
		return longValue / unit;
	}
	else {
		return (longValue + 1) / unit - 1;
	}
}

std::pair<int64_t, int64_t> SQLExprs::DataPartitionUtils::rawIntervalToLong(
		int64_t rawInterval, int64_t unit) {
	if (unit <= 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const int64_t min = std::numeric_limits<int64_t>::min();
	const int64_t max = std::numeric_limits<int64_t>::max();

	int64_t lower;
	int64_t upper;
	if (rawInterval >= 0) {
		const int64_t cur = rawInterval;
		if (cur > max / unit) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		const int64_t begin = cur * unit;
		const int64_t offset = unit - 1;

		lower = begin;
		if (begin > max - offset) {
			upper = max;
		}
		else {
			upper = begin + offset;
		}
	}
	else {
		const int64_t next = rawInterval + 1;
		if (next < min / unit) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		const int64_t end = next * unit;

		if (end < min + unit) {
			lower = min;
		}
		else {
			lower = end - unit;
		}
		upper = end - 1;
	}
	return std::make_pair(lower, upper);
}

int64_t SQLExprs::DataPartitionUtils::intervalValueToLong(
		const TupleValue &value) {
	int64_t longValue;
	if (!SQLValues::ValueUtils::findLong(value, longValue)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return longValue;
}

int64_t SQLExprs::DataPartitionUtils::intervalToAffinity(
		SyntaxTree::TablePartitionType partitioningType,
		uint32_t partitioningCount, uint32_t clusterPartitionCount,
		const util::Vector<int64_t> &affinityList,
		int64_t interval, uint32_t hash) {

	const uint32_t count = clusterPartitionCount;
	if (count <= 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	int64_t base;
	int64_t affinityIndex;
	int64_t offset;
	switch (partitioningType) {
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
		base = hash / count * count + count;
		affinityIndex = hash % count;
		offset = 0;
		break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
		base = interval / (count * 2) * (count * 2) + count;
		affinityIndex = ((interval >> 1) % count);
		offset = ((interval & 0x01) ? count : 0);
		break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
		base = interval / (count * 2) * (count * 2) * partitioningCount +
				hash + count;
		affinityIndex = ((interval >> 1) % count);
		offset = ((interval & 0x01) ? count * partitioningCount : 0);
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	if (affinityIndex < 0 ||
			static_cast<size_t>(affinityIndex) >= affinityList.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return base + affinityList[static_cast<size_t>(affinityIndex)] + offset;
}

std::pair<int64_t, uint32_t>
SQLExprs::DataPartitionUtils::intervalHashFromAffinity(
		SyntaxTree::TablePartitionType partitioningType,
		uint32_t partitioningCount, uint32_t clusterPartitionCount,
		const util::Vector<uint32_t> &affinityRevList, int64_t affinity) {

	int64_t unit;
	uint32_t indexCount;
	switch (partitioningType) {
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
		unit = 1;
		indexCount = static_cast<uint32_t>(std::min(
				partitioningCount, clusterPartitionCount));
		break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
		unit = 1;
		indexCount = clusterPartitionCount;
		break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
		unit = partitioningCount;
		indexCount = clusterPartitionCount;
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const int64_t count = static_cast<int64_t>(clusterPartitionCount);
	if (count <= 0 || affinity < count ||
			affinityRevList.size() != clusterPartitionCount) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const int64_t diff = affinity - count;
	const int64_t base = diff / unit;

	const uint32_t index =
			affinityRevList[static_cast<uint32_t>(base % count)];
	if (index >= indexCount) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	int64_t interval;
	uint32_t hash;
	if (partitioningType == SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
		if (base / count * count >=
				std::numeric_limits<int64_t>::max() - indexCount) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		interval = 0;
		hash = static_cast<uint32_t>(base / count * count + index);
	}
	else {
		if (base / (count * 2) * (count * 2) >=
				std::numeric_limits<int64_t>::max() - (count * 2)) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		interval = base / (count * 2) * (count * 2) + index * 2 +
				((base / count) % 2 == 0 ? 0 : 1);
		hash = static_cast<uint32_t>(diff % unit);
	}

	return std::make_pair(interval, hash);
}

bool SQLExprs::DataPartitionUtils::isFixedIntervalPartitionAffinity(
		SyntaxTree::TablePartitionType partitioningType,
		uint32_t clusterPartitionCount, int64_t affinity) {
	switch (partitioningType) {
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
		break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
		break;
	default:
		return false;
	}

	return (affinity >= 0 &&
			static_cast<uint64_t>(affinity) < clusterPartitionCount);
}

void SQLExprs::DataPartitionUtils::makeAffinityRevList(
		SyntaxTree::TablePartitionType partitioningType,
		uint32_t partitioningCount,  uint32_t clusterPartitionCount,
		const util::Vector<int64_t> &affinityList,
		util::Vector<uint32_t> &affinityRevList) {
	int64_t unit;
	uint32_t inSize;
	uint32_t outSize;
	switch (partitioningType) {
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
		unit = 1;
		inSize = static_cast<uint32_t>(std::min(
				partitioningCount, clusterPartitionCount));
		outSize = clusterPartitionCount;
		break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
		unit = 1;
		inSize = clusterPartitionCount;
		outSize = inSize;
		break;
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
		unit = partitioningCount;
		inSize = clusterPartitionCount;
		outSize = inSize;
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	const uint32_t max = std::numeric_limits<uint32_t>::max();

	affinityRevList.resize(outSize, max);

	if (affinityList.size() != inSize || inSize <= 0 || outSize >= max) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	for (util::Vector<int64_t>::const_iterator it = affinityList.begin();
			it != affinityList.end(); ++it) {
		const int64_t revIndex = *it / unit;
		if (*it < 0 || *it % unit != 0 ||
				static_cast<size_t>(revIndex) >= outSize) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		uint32_t &affinityRev = affinityRevList[static_cast<size_t>(revIndex)];

		if (affinityRev < outSize) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}
		affinityRev = static_cast<uint32_t>(it - affinityList.begin());
	}
}


bool SQLExprs::RangeGroupUtils::isRangeGroupSupportedType(
		TupleColumnType type) {
	return SQLValues::TypeUtils::isTimestampFamily(type);
}

void SQLExprs::RangeGroupUtils::resolveRangeGroupInterval(
		TupleColumnType type, int64_t baseInterval, int64_t baseOffset,
		util::DateTime::FieldType unit, const util::TimeZone &timeZone,
		RangeKey &interval, RangeKey &offset) {
	RangeKey outInterval =
			resolveRangeGroupIntervalValue(baseInterval, unit, true);
	RangeKey outOffset =
			resolveRangeGroupIntervalValue(baseOffset, unit, false);
	adjustRangeGroupInterval(outInterval, outOffset, timeZone);

	checkRangeGroupInterval(type, outInterval);

	interval = outInterval;
	offset = outOffset;
}

SQLExprs::RangeKey SQLExprs::RangeGroupUtils::resolveRangeGroupIntervalValue(
		int64_t baseInterval, util::DateTime::FieldType unit,
		bool forInterval) {
	util::DateTime::ZonedOption option;
	util::PreciseDateTime dt;
	try {
		dt.addField(baseInterval, unit, option);
	}
	catch (util::UtilityException &e) {
		const char8_t *target = (forInterval ? "interval" : "offset");
		GS_RETHROW_USER_ERROR_CODED(
				GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
				e, "Too large range group " << target <<" (reason=" <<
				e.getField(util::Exception::FIELD_MESSAGE) << ")");
	}
	return SQLValues::DateTimeElements(dt).toLongInt();
}

void SQLExprs::RangeGroupUtils::adjustRangeGroupInterval(
		RangeKey &interval, RangeKey &offset, const util::TimeZone &timeZone) {
	const RangeKey::Unit &keyUnit = interval.getUnit();

	RangeKey outInterval = interval.getMax(RangeKey::ofElements(0, 1, keyUnit));
	RangeKey outOffset =
			offset.getMax(RangeKey::zero(keyUnit)).remainder(outInterval);

	if (!timeZone.isEmpty()) {
		const RangeKey zoneOffset =
				RangeKey::ofElements(timeZone.getOffsetMillis(), 0, keyUnit);

		const RangeKey diff = (zoneOffset.isNegative() ?
				outInterval.subtract(
						zoneOffset.negate(true).remainder(outInterval)) :
				zoneOffset.remainder(outInterval));

		if (outOffset.isLessThan(outInterval.subtract(diff))) {
			outOffset = outOffset.add(diff);
		}
		else {
			outOffset = outOffset.add(diff.subtract(outInterval));
		}
	}

	interval = outInterval;
	offset = outOffset;
}

void SQLExprs::RangeGroupUtils::checkRangeGroupInterval(
		TupleColumnType type, const RangeKey &interval) {
	const RangeKey min = getRangeGroupMinInterval(type);
	if (interval.isLessThan(min)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
				"Too small range group interval for the grouping key (keyType=" <<
				SQLValues::TypeUtils::toString(
						SQLValues::TypeUtils::toNonNullable(type)) << ")");
	}
}

SQLExprs::RangeKey SQLExprs::RangeGroupUtils::getRangeGroupMinInterval(
		TupleColumnType type) {
	util::PreciseDateTime base;
	switch (SQLValues::TypeUtils::toNonNullable(type)) {
	case TupleTypes::TYPE_TIMESTAMP:
		base = util::PreciseDateTime::ofNanoSeconds(util::DateTime(1), 0);
		break;
	case TupleTypes::TYPE_MICRO_TIMESTAMP:
		base = util::PreciseDateTime::ofNanoSeconds(util::DateTime(0), 1000);
		break;
	case TupleTypes::TYPE_NANO_TIMESTAMP:
		base = util::PreciseDateTime::ofNanoSeconds(util::DateTime(0), 1);
		break;
	default:
		return RangeKey::invalid();
	}
	return SQLValues::DateTimeElements(base).toLongInt();
}

bool SQLExprs::RangeGroupUtils::findRangeGroupBoundary(
		util::StackAllocator &alloc, TupleColumnType keyType,
		const TupleValue &base, bool forLower, bool inclusive,
		RangeKey &boundary) {
	boundary = RangeKey::invalid();

	if (SQLValues::ValueUtils::isNull(base)) {
		return false;
	}

	const TupleColumnType promoType = SQLValues::TypeUtils::toNonNullable(
			SQLValues::TypeUtils::findPromotionType(
					keyType, base.getType(), true));
	if (SQLValues::TypeUtils::isNull(promoType)) {
		return false;
	}

	util::StackAllocator::Scope allocScope(alloc);
	SQLValues::ValueContext valueCxt(
			SQLValues::ValueContext::ofAllocator(alloc));
	const TupleValue promoBase =
			SQLValues::ValueConverter(promoType)(valueCxt, base);

	const RangeKey longBase = SQLValues::ValueUtils::toLongInt(promoBase);
	const RangeKey::Unit unit = longBase.getUnit();

	const bool found = true;
	RangeKey gap = RangeKey::zero(unit);
	if (!inclusive) {
		if (forLower) {
			const RangeKey &max =
					SQLValues::ValueUtils::getMaxTimestamp().toLongInt();
			if (max.isLessThanEq(longBase)) {
				return found;
			}
		}
		else {
			const RangeKey &min =
					SQLValues::ValueUtils::getMinTimestamp().toLongInt();
			if (longBase.isLessThanEq(min)) {
				return found;
			}
		}
		gap = RangeKey::ofElements(0, 1, unit).negate(!forLower);
	}

	boundary = longBase.add(gap);
	return found;
}

bool SQLExprs::RangeGroupUtils::adjustRangeGroupBoundary(
		RangeKey &lower, RangeKey &upper, const RangeKey &interval,
		const RangeKey &offset) {
	assert(!offset.isNegative());
	assert(offset.isLessThan(interval));

	bool normal = true;
	do {
		if (!lower.isValid() || !upper.isValid()) {
			break;
		}

		if (upper.isLessThan(lower)) {
			break;
		}

		for (size_t i = 0; i < 2; i++) {
			RangeKey &baseRef = (i == 0 ? lower : upper);
			const RangeKey base = baseRef;

			RangeKey gap = RangeKey::zero(base.getUnit());
			if (base.isLessThan(offset)) {
				gap = interval.negate(!base.isLessThan(interval));
				normal = false;
			}

			const RangeKey sub = base.subtract(offset).add(gap);
			baseRef = sub.truncate(interval).add(offset);

			assert(SQLValues::ValueUtils::getMinTimestamp().toLongInt(
					).isLessThanEq(baseRef));
			assert(baseRef.isLessThanEq(
					SQLValues::ValueUtils::getMaxTimestamp().toLongInt()));
		}
		return normal;
	}
	while (false);

	const RangeKey::Unit unit = interval.getUnit();
	const RangeKey emptyLower = RangeKey::ofElements(1, 0, unit);
	const RangeKey emptyUpper = RangeKey::ofElements(0, 0, unit);
	lower = emptyLower;
	upper = emptyUpper;
	return normal;
}

TupleValue SQLExprs::RangeGroupUtils::mergeRangeGroupBoundary(
		const TupleValue &base1, const TupleValue &base2, bool forLower) {
	if (SQLValues::ValueUtils::isNull(base1)) {
		return base2;
	}
	else if (SQLValues::ValueUtils::isNull(base2)) {
		return base1;
	}

	const bool sensitive = false;
	const bool ascending = forLower;
	const bool ordered = (SQLValues::ValueComparator::ofValues(
			base1, base2, sensitive, ascending)(base1, base2) < 0);

	return (ordered ? base2 : base1);
}

bool SQLExprs::RangeGroupUtils::getRangeGroupId(
		const TupleValue &key, const RangeKey &interval,
		const RangeKey &offset, RangeKey &id) {
	assert(SQLValues::LongInt::zero(offset.getUnit()).isLessThanEq(offset));
	assert(offset.isLessThan(interval));

	if (SQLValues::ValueUtils::isNull(key)) {
		id = RangeKey::invalid();
		return false;
	}

	const RangeKey base = SQLValues::ValueUtils::toLongInt(key);
	const RangeKey gap = (
			offset.isLessThanEq(base) ? offset : offset.subtract(interval));

	id = base.subtract(gap).truncate(interval).add(gap);
	return true;
}


const SQLExprs::ExprFactory& SQLExprs::DefaultExprFactory::factory_ =
		getFactoryForRegistrar();

const SQLExprs::ExprFactory& SQLExprs::DefaultExprFactory::getFactory() {
	return factory_;
}

SQLExprs::DefaultExprFactory&
SQLExprs::DefaultExprFactory::getFactoryForRegistrar() {
	const size_t entryCount = SQLType::END_TYPE;
	static Entry entryList[entryCount];
	static DefaultExprFactory factory(entryList, entryCount);
	return factory;
}

SQLExprs::Expression& SQLExprs::DefaultExprFactory::create(
		ExprFactoryContext &cxt, const ExprCode &code) const {
	Expression *expr;

	if (cxt.isPlanning()) {
		expr = ALLOC_NEW(cxt.getAllocator()) PlanningExpression(cxt, code);
	}
	else {
		const bool overwriting = false;
		const ExprType type = code.getType();
		FactoryFunc func = entryList_[getEntryIndex(type, overwriting)].func_;

		if (func == NULL) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
		}

		expr = &(func(cxt, code));
	}

	expr->initialize(cxt, code);

	return *expr;
}

const SQLExprs::ExprSpec& SQLExprs::DefaultExprFactory::getSpec(
		ExprType type) const {
	const bool overwriting = false;
	return entryList_[getEntryIndex(type, overwriting)].spec_;
}

void SQLExprs::DefaultExprFactory::add(
		ExprType type, const ExprSpec &spec, FactoryFunc func) {
	const bool overwriting = true;
	Entry &entry = entryList_[getEntryIndex(type, overwriting)];

	if (entry.type_ != SQLType::START_EXPR) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	entry.type_ = type;
	entry.spec_ = spec;
	entry.func_ = func;

	const ExprType distinctType = spec.distinctExprType_;
	if (distinctType != SQLType::START_EXPR && distinctType != type &&
			!SQLExprs::ExprRewriter::isDistinctAggregation(spec, type)) {
		add(distinctType, spec.toDistinct(type), NULL);
	}
}

SQLExprs::DefaultExprFactory::DefaultExprFactory(
		Entry *entryList, size_t entryCount) :
		entryList_(entryList),
		entryCount_(entryCount) {
}

size_t SQLExprs::DefaultExprFactory::getEntryIndex(
		ExprType type, bool overwriting) const {
	do {
		if (type < 0 || type == SQLType::START_EXPR) {
			break;
		}
		const size_t index = static_cast<size_t>(type);
		if (index >= entryCount_) {
			break;
		}
		if (!overwriting && entryList_[index].type_ == SQLType::START_EXPR) {
			break;
		}
		return index;
	}
	while (false);

	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}

SQLExprs::DefaultExprFactory::Entry::Entry() :
		type_(SQLType::START_EXPR),
		func_(NULL) {
}


SQLExprs::ExprRegistrar::ExprRegistrar() throw() :
		factory_(&DefaultExprFactory::getFactoryForRegistrar()) {
}

SQLExprs::ExprRegistrar::ExprRegistrar(
		const ExprRegistrar &subRegistrar) throw() {
	try {
		subRegistrar();
	}
	catch (...) {
		assert(false);
	}
}

void SQLExprs::ExprRegistrar::operator()() const {
	assert(false);
}

void SQLExprs::ExprRegistrar::addDirect(
		ExprType type, const ExprSpec &spec, FactoryFunc func) const {
	assert(factory_ != NULL);
	factory_->add(type, spec, func);
}

SQLExprs::ExprRegistrar::FactoryFunc
SQLExprs::ExprRegistrar::resolveFactoryFunc(
		ExprFactoryContext &cxt, const BasicFuncTable &table,
		const ExprSpec &spec) {
	if (cxt.isArgChecking()) {
		return table.checker_;
	}
	const size_t countVariant = getArgCountVariant(cxt, spec);
	const size_t typeVariant = getColumnTypeVariant(cxt, spec);
	return table.base_.list_[countVariant].list_[typeVariant];
}

SQLExprs::ExprRegistrar::FactoryFunc
SQLExprs::ExprRegistrar::resolveFactoryFunc(
		ExprFactoryContext &cxt, const AggrFuncTable &table,
		const ExprSpec &spec) {
	if (cxt.isArgChecking()) {
		return table.checker_;
	}
	const FuncTableByTypes &subTable =
			resolveAggregationSubTable(cxt, table.base_);
	const size_t typeVariant = getColumnTypeVariant(cxt, spec);
	return subTable.list_[typeVariant];
}

const SQLExprs::ExprRegistrar::FuncTableByTypes&
SQLExprs::ExprRegistrar::resolveAggregationSubTable(
		ExprFactoryContext &cxt, const FuncTableByAggr &baseTable) {
	switch (cxt.getAggregationPhase(false)) {
	case SQLType::AGG_PHASE_ADVANCE_PIPE:
		return resolveDecrementalSubTable(cxt, baseTable);
	case SQLType::AGG_PHASE_ADVANCE_FINISH:
		return baseTable.finish_;
	case SQLType::AGG_PHASE_MERGE_PIPE:
		return baseTable.merge_;
	case SQLType::AGG_PHASE_MERGE_FINISH:
		return baseTable.finish_;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

const SQLExprs::ExprRegistrar::FuncTableByTypes&
SQLExprs::ExprRegistrar::resolveDecrementalSubTable(
		ExprFactoryContext &cxt, const FuncTableByAggr &baseTable) {
	switch (cxt.getDecrementalType()) {
	case ExprSpec::DECREMENTAL_NONE:
		return baseTable.advance_;
	case ExprSpec::DECREMENTAL_FORWARD:
		return baseTable.decrForward_;
	case ExprSpec::DECREMENTAL_BACKWARD:
		return baseTable.decrBackward_;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
}

size_t SQLExprs::ExprRegistrar::getArgCountVariant(
		ExprFactoryContext &cxt, const ExprSpec &spec) {
	const Expression &baseExpr = resolveBaseExpression(cxt);

	const size_t argCount = baseExpr.getChildCount();
	const size_t minCount = static_cast<size_t>(spec.argCounts_.first);

	if (argCount < minCount) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	size_t opt;
	const int32_t maxCount = spec.argCounts_.second;
	if (maxCount >= 0) {
		const size_t diff = argCount - minCount;
		const size_t maxOpt = static_cast<size_t>(maxCount) - minCount;
		opt = static_cast<size_t>(std::min<uint64_t>(diff, maxOpt));
	}
	else {
		opt = 0;
	}

	if (opt >= ExprSpec::IN_MAX_OPTS_COUNT) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	return opt;
}

size_t SQLExprs::ExprRegistrar::getColumnTypeVariant(
		ExprFactoryContext &cxt, const ExprSpec &spec) {
	const Expression &baseExpr = resolveBaseExpression(cxt);

	const AggregationPhase aggrPhase = cxt.getAggregationPhase(false);
	const bool byAggr = spec.isAggregation() &&
			(aggrPhase == SQLType::AGG_PHASE_MERGE_PIPE ||
			aggrPhase == SQLType::AGG_PHASE_MERGE_FINISH);

	uint32_t aggrIndex = baseExpr.getCode().getAggregationIndex();
	const Expression *subExpr = NULL;
	int32_t lastIndex = -1;
	for (uint32_t i = 0;; i++) {
		const ExprSpec::In *in;
		if (byAggr) {
			in = (i < ExprSpec::AGGR_LIST_SIZE ? &spec.aggrList_[i] : NULL);
		}
		else {
			in = (i < ExprSpec::IN_LIST_SIZE ? &spec.inList_[i] : NULL);
			subExpr = (subExpr == NULL ?
					baseExpr.findChild() : subExpr->findNext());
			if (subExpr == NULL) {
				break;
			}
		}

		if (in == NULL || SQLValues::TypeUtils::isNull(in->typeList_[0])) {
			break;
		}

		TupleColumnType type;
		if (byAggr) {
			type = cxt.getAggregationColumnType(aggrIndex + i);
		}
		else {
			type = subExpr->getCode().getColumnType();
		}
		type = SQLValues::TypeUtils::toNonNullable(type);

		bool definitive;
		if (ExprSpecBase::InPromotionVariants::matchVariantIndex(
				*in, type, lastIndex, definitive)) {
			if (definitive) {
				return static_cast<size_t>(lastIndex);
			}
			continue;
		}

		for (size_t j = 0; j < ExprSpec::IN_TYPE_COUNT; j++) {
			const TupleColumnType inType = in->typeList_[j];
			if (SQLValues::TypeUtils::isNull(inType)) {
				break;
			}
			else if (j > 0 && inType == type) {
				return j;
			}
		}
	}
	if (lastIndex >= 0) {
		return lastIndex;
	}
	return 0;
}

const SQLExprs::Expression& SQLExprs::ExprRegistrar::resolveBaseExpression(
		ExprFactoryContext &cxt) {
	const Expression *baseExpr = cxt.getBaseExpression();
	if (baseExpr == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}
	return *baseExpr;
}

SQLExprs::ExprRegistrar::FuncTableByTypes::FuncTableByTypes() {
	std::fill(list_, list_ + LIST_SIZE, FactoryFunc());
}

SQLExprs::ExprRegistrar::FuncTableByTypes::FuncTableByTypes(
		const FactoryFunc (&list)[LIST_SIZE]) {
	for (size_t i = 0; i < LIST_SIZE; i++) {
		list_[i] = list[i];
	}
}

SQLExprs::ExprRegistrar::FuncTableByCounts::FuncTableByCounts(
		const FuncTableByTypes (&list)[LIST_SIZE]) {
	for (size_t i = 0; i < LIST_SIZE; i++) {
		list_[i] = list[i];
	}
}

SQLExprs::ExprRegistrar::FuncTableByAggr::FuncTableByAggr(
		const FuncTableByTypes &advance,
		const FuncTableByTypes &merge,
		const FuncTableByTypes &finish,
		const FuncTableByTypes &decrForward,
		const FuncTableByTypes &decrBackward) :
		advance_(advance),
		merge_(merge),
		finish_(finish),
		decrForward_(decrForward),
		decrBackward_(decrBackward) {
}

SQLExprs::ExprRegistrar::BasicFuncTable::BasicFuncTable(
		const FuncTableByCounts base, FactoryFunc checker) :
		base_(base),
		checker_(checker) {
}

SQLExprs::ExprRegistrar::AggrFuncTable::AggrFuncTable(
		const FuncTableByAggr base, FactoryFunc checker) :
		base_(base),
		checker_(checker) {
}


bool SQLExprs::ExprSpecBase::InPromotionVariants::matchVariantIndex(
		const ExprSpec::In &in, TupleColumnType type,
		int32_t &lastIndex, bool &definitive) {
	assert(!SQLValues::TypeUtils::isNullable(type));

	if (isNumericPromotion(in)) {
		if (SQLValues::TypeUtils::isFloating(type)) {
			lastIndex = 1;
			definitive = true;
		}
		else {
			lastIndex = 0;
			definitive = false;
		}
		return true;
	}
	else if (isTimestampPromotion(in)) {
		int32_t baseIndex;
		switch (type) {
		case TupleTypes::TYPE_TIMESTAMP:
			baseIndex = 0;
			break;
		case TupleTypes::TYPE_MICRO_TIMESTAMP:
			baseIndex = 1;
			break;
		case TupleTypes::TYPE_NANO_TIMESTAMP:
			baseIndex = 2;
			break;
		default:
			definitive = false;
			return false;
		}
		lastIndex = std::max(lastIndex, baseIndex);
		definitive = (lastIndex >= 2);
		return true;
	}
	else {
		definitive = false;
		return false;
	}
}

bool SQLExprs::ExprSpecBase::InPromotionVariants::isNumericPromotion(
		const ExprSpec::In &in) {
	const TupleColumnType inType = in.typeList_[0];
	return (inType == TupleTypes::TYPE_NUMERIC);
}

bool SQLExprs::ExprSpecBase::InPromotionVariants::isTimestampPromotion(
		const ExprSpec::In &in) {
	const TupleColumnType inType = in.typeList_[0];
	return (inType == PromotableTimestampIn::TypeAt<0>::COLUMN_TYPE &&
			(in.flags_ & ExprSpec::FLAG_PROMOTABLE) != 0);
}


int64_t SQLExprs::ExprUtils::AggregationManipulator<int64_t>::add(
		void*, int64_t v1, int64_t v2, const util::TrueType&) {
	return FunctionUtils::NumberArithmetic::add(v1, v2);
}

double SQLExprs::ExprUtils::AggregationManipulator<double>::add(
		void*, double v1, double v2, const util::TrueType&) {
	return FunctionUtils::NumberArithmetic::add(v1, v2);
}


const SQLExprs::ExprUtils::EmptyComparatorRef::BaseType&
SQLExprs::ExprUtils::EmptyComparatorRef::get() const {
	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}


SQLExprs::PlanningExpression::PlanningExpression(
		ExprFactoryContext &cxt, const ExprCode &code) :
		code_(code) {
	static_cast<void>(cxt);
	setPlanningCode(&code_);
}

TupleValue SQLExprs::PlanningExpression::eval(ExprContext &cxt) const {
	static_cast<void>(cxt);
	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
}


SQLExprs::ComparableExpressionBase::ComparableExpressionBase() :
		comparator_(NULL) {
}

void SQLExprs::ComparableExpressionBase::initializeCustom(
		ExprFactoryContext &cxt, const ExprCode &code) {
	const ExprSpec &spec = cxt.getFactory().getSpec(code.getType());
	comparator_ = arrangeComparator(cxt, spec);
}

const SQLValues::ValueComparator::Arranged*
SQLExprs::ComparableExpressionBase::arrangeComparator(
		ExprFactoryContext &cxt, const ExprSpec &spec) {
	if (cxt.isPlanning()) {
		return NULL;
	}

	if (spec.isAggregation() && SQLExprs::ExprRewriter::checkArgCount(
			spec, SQLType::START_EXPR, 2, SQLType::AGG_PHASE_ALL_PIPE, false)) {
		return NULL;
	}

	const Expression *base = cxt.getBaseExpression();
	const Expression *arg1 = base->findChild();
	if (arg1 == NULL) {
		return NULL;
	}

	const Expression *arg2 = arg1->findNext();

	const TupleColumnType columnType1 = arg1->getCode().getColumnType();
	const TupleColumnType columnType2 = (arg2 == NULL ?
			columnType1 : arg2->getCode().getColumnType());

	SQLValues::ValueAccessor accessor1(columnType1);
	SQLValues::ValueAccessor accessor2(columnType2);

	const bool sensitive = ((spec.flags_ & ExprSpec::FLAG_COMP_SENSITIVE) != 0);

	return ALLOC_NEW(cxt.getAllocator()) SQLValues::ValueComparator::Arranged(
			SQLValues::ValueComparator(accessor1, accessor2, NULL, sensitive, true));
}


SQLExprs::AggregationExpressionBase::AggregationExpressionBase() :
		comparator_(NULL) {
}

void SQLExprs::AggregationExpressionBase::initializeCustom(
		ExprFactoryContext &cxt, const ExprCode &code) {
	if (cxt.isArgChecking()) {
		return;
	}

	const ExprSpec &spec = cxt.getFactory().getSpec(code.getType());

	uint32_t count = 0;
	for (uint32_t i = 0; i < ExprSpec::AGGR_LIST_SIZE; i++) {
		if (!SQLValues::TypeUtils::isNull(spec.aggrList_[i].typeList_[0])) {
			count = i + 1;
		}
	}
	assert(count > 0);

	const uint32_t aggrIndex = code.getAggregationIndex();
	for (uint32_t i = 0; i < count; i++) {
		aggrColumns_[i] = cxt.getArrangedAggregationColumn(aggrIndex + i);
	}

	comparator_ = ComparableExpressionBase::arrangeComparator(cxt, spec);
}
