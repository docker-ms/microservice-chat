'use strict';

const CommonImport = require('../../util/CommonImport');

class _DeleteConversationImpl {

  static deleteConversation(call, callback) {

    CommonImport.utils.bluebirdRetryExecutor(() => {
      const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
      const conversationsCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.conversationsCollectionName);
      return conversationsCollection.deleteOne({
        [`${call.request.id}`]: call.request[call.request.id]
      });
    }, {}).then((res) => {
      if (res.deletedCount === 1) {
        callback(null, {success: true});
      } else {
        return CommonImport.Promise.reject(new CommonImport.errors.UnknownError());
      }
    }).catch((err) => {
      CommonImport.utils.apiImplCommonErrorHandler(err, CommonImport.errors, callback);
    });

  }

}

module.exports = _DeleteConversationImpl;


