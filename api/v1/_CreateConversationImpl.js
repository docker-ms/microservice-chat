'use strict';

const CommonImport = require('../../util/CommonImport');

class _CreateConversationImpl {

  static createConversation(call, callback) {

    const now = Date.now();

    call.request.conversationType = CommonImport.protos.enums.conversationTypes[call.request.conversationType];
    
    const conversation = {
      conversationId: call.request.conversationId,
      conversationType: call.request.conversationType,
      conversationStatus: CommonImport.protos.enums.conversationStatuses.ACCESSIBLE,
      creator: call.request.creatorUserId,
      lastUpdate: now,
      createAt: now,
      tester: call.request.tester
    };

    // For: 'ONE2ONE', 'TEMP_ONE2ONE', 'TEMP_GROUP'.
    if (call.request.conversationType !== CommonImport.protos.enums.conversationTypes.GROUP) {

      CommonImport.utils.bluebirdRetryExecutor(() => {
        const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
        const usersCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.usersCollectionName);
        return usersCollection.find({
          userId: {
            $in: call.request.memberUserIds
          },
          userStatus: CommonImport.protos.enums.userStatuses.ACTIVE
        }).toArray().then((res) => {
          if (res.length === call.request.memberUserIds.length) {

            switch (call.request.conversationType) {
              case CommonImport.protos.enums.conversationTypes.ONE2ONE:
                if (res[0].confirmedContacts.indexOf(res[1].userId) === -1) {
                  return CommonImport.Promise.reject(new CommonImport.errors.NoPermission.ToBeContactsFirst());
                }
                break;
              case CommonImport.protos.enums.conversationTypes.TEMP_ONE2ONE:
                if (!call.request.isTemp121ConversationEnabled) {
                  return CommonImport.Promise.reject(new CommonImport.errors.BusinessLogic.Temp121ConversationIsForbidden());
                } else if (res[0].confirmedContacts.indexOf(res[1].userId) !== -1) {
                  return CommonImport.Promise.reject(new CommonImport.errors.UncategorizedError.InvalidRequest());
                }
                break;
              case CommonImport.protos.enums.conversationTypes.TEMP_GROUP:
                if (!call.request.isTempGroupEnabled) {
                  return CommonImport.Promise.reject(new CommonImport.errors.BusinessLogic.TempGroupIsForbidden());
                } else if (res.length < 3) {
                  return CommonImport.Promise.reject(new CommonImport.errors.BusinessLogic.TempGroupAtLeaseHas3Peers());
                }
                break;
            }

            const epochNow = +new Date();

            conversation.members = call.request.memberUserIds.reduce((acc, curr) => {
              acc[curr] = {
                joinInAt: epochNow
              };
              return acc;
            }, {});
            return CommonImport.utils.bluebirdRetryExecutor(() => {
              const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
              const conversationsCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.conversationsCollectionName);
              const queryObj = call.request.memberUserIds.reduce((acc, curr) => {
                acc[`members.${curr}`] = {
                  $exists: true
                }
                return acc;
              }, {
                conversationType: call.request.conversationType
              });
              return conversationsCollection.findOne(queryObj, {
                fields: {
                  conversationId: 1,
                  members: 1
                }
              });
            }, {});
          } else {
            return CommonImport.Promise.reject(new CommonImport.errors.ResourceNotFound.ActiveUserNotFound());
          }
        });
      }, {}).then((res) => {
        if (res && Object.keys(res.members).length === call.request.memberUserIds.length) {
          return CommonImport.Promise.resolve(res);
        } else {
          return CommonImport.utils.bluebirdRetryExecutor(() => {
            const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
            const conversationsCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.conversationsCollectionName);
            return conversationsCollection.insertOne(conversation);
          }, {});
        }
      }).then((res) => {
        let returnedConversationId;
        if (res.conversationId) {
          returnedConversationId = res.conversationId;
          // TODO: need to record 'InvalidApiUsageDetected' error.
        } else if (res.ops[0]) {
          returnedConversationId = res.ops[0].conversationId;
        } else {
          returnedConversationId = conversation.conversationId;
        }

        /*
         * Update user's `activeConversations` list.
         */
        return CommonImport.utils.bluebirdRetryExecutor(() => {
          const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
          const usersCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.usersCollectionName);
          return usersCollection.updateMany({
            userId: {
              $in: call.request.memberUserIds
            }
          }, {
            $addToSet: {
              activeConversations: returnedConversationId
            }
          });
        }, {}).then(() => {
          callback(null, {conversationId: returnedConversationId})
        });
      }).catch((err) => {
        CommonImport.utils.apiImplCommonErrorHandler(err, CommonImport.errors, callback);
      });

    } else {

      conversation.forGroupId = call.request.conversationId;
      
      CommonImport.utils.bluebirdRetryExecutor(() => {
        const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
        const conversationsCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.conversationsCollectionName);
        return conversationsCollection.insertOne(conversation);
      }, {}).then((res) => {
        callback(null)
      }).catch((err) => {
        CommonImport.utils.apiImplCommonErrorHandler(err, CommonImport.errors, callback);
      });

    }

  }

}

module.exports = _CreateConversationImpl;


